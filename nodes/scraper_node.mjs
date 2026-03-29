/**
 * ScraperNode — Feeds fresh leads into all other swarm nodes.
 *
 * Capabilities:
 *   - Google Search + Puppeteer: finds local businesses, scrapes sites for
 *     emails/phone, detects existing chatbots (FREE, unlimited)
 *   - Hunter Discover: finds companies by location, industry, headcount (FREE)
 *   - Hunter Domain Search: extracts verified emails from company domains
 *   - Apollo.io (paid plans): bulk people search by title/industry/location
 *   - Reddit: finds pain-point signals from relevant subreddits
 *   - Chatbot detection: skips businesses that already have a chat widget
 *   - Deduplication before writing to SharedMemory
 *   - Configurable targeting filters (industry, title, company size, location)
 */

import axios from 'axios';
import * as cheerio from 'cheerio';
import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import { BaseNode } from '../core/base_node.mjs';
import { log } from '../core/logger.mjs';

puppeteer.use(StealthPlugin());

// Chat widget script/element signatures to detect existing chatbots
const CHATBOT_SIGNATURES = [
  // Script src patterns
  'intercom', 'drift.com', 'tidio.co', 'livechat', 'livechatinc',
  'zendesk', 'zopim', 'freshchat', 'freshdesk', 'crisp.chat',
  'tawk.to', 'olark', 'chatwoot', 'botpress', 'hubspot',
  'birdeye', 'podium', 'webchat', 'manychat', 'chatbot.com',
  'kommunicate', 'smartsupp', 'jivochat', 'purechat', 'chatra',
  'userlike', 'gorgias', 'helpcrunch', 'customerly', 'acquire.io',
  'smith.ai', 'chatfuel', 'landbot', 'collect.chat', 'activechat',
  // Heinrichs competitors / generic widget loaders
  'widget.js', 'chat-widget', 'chatwidget', 'live-chat',
];

const sleep = ms => new Promise(r => setTimeout(r, ms));

export class ScraperNode extends BaseNode {
  static nodeType = 'scraper_node';

  constructor(config, region = 'us-east-1', parentId = null) {
    super(config, region, parentId);
    this.apolloApiKey    = config.apollo_api_key ?? '';
    this.hunterApiKey    = config.hunter_api_key ?? '';
    this.targetTitles    = config.target_titles ?? ['CEO', 'Founder', 'CMO', 'VP Marketing'];
    this.targetIndustries = config.target_industries ?? [];
    this.targetLocations = config.target_locations ?? [];
    this.minCompanySize  = config.min_company_size ?? 1;
    this.maxCompanySize  = config.max_company_size ?? 500;
    this.leadsPerCycle   = config.leads_per_cycle ?? 50;
    this.sources         = config.sources ?? ['web_scrape', 'hunter_discover', 'reddit'];

    // Hunter Discover params (mapped from profile ICP)
    this.hunterIndustries = config.hunter_industries ?? [];
    this.hunterHeadcount  = config.hunter_headcount  ?? ['1-10', '11-50'];
    this.hunterLocations  = config.hunter_locations   ?? [];      // [{ country, state, city }]

    // Google scrape settings
    this.dailyCapPerCity  = config.daily_cap_per_city ?? 200;     // max businesses to scrape per city/day
    this._scrapedToday    = new Map();                             // city → count
    this._scrapedDomains  = new Set();                             // global dedup across cycles
    this._browser         = null;                                  // shared Puppeteer browser

    // Track which domains we've already searched via Hunter (to avoid burning search credits)
    this._searchedDomains = new Set();

    // Pre-warm domain/email cache — skip known businesses without scraping
    this._knownDomains      = new Set();
    this._cacheWarmed       = false;
    this._cycleCount        = 0;
    this._cacheRefreshEvery = config.cache_refresh_every ?? 10;
  }

  async runCycle() {
    const tasks = await this.receiveTasks(10);
    for (const t of tasks) {
      await this._handleTask(t);
      await this.ackTask(t._receipt_handle);
    }

    this._cycleCount++;
    if (!this._cacheWarmed || this._cycleCount % this._cacheRefreshEvery === 0) {
      await this._warmDomainCache();
    }

    await this._scrapeAndStore();
  }

  async _warmDomainCache() {
    try {
      const existing = await this.memory.getLeads(null, 10000);
      this._knownDomains.clear();
      for (const lead of existing) {
        if (lead.website) this._knownDomains.add(lead.website.toLowerCase().replace(/^www\./, ''));
        if (lead.email)   this._knownDomains.add(lead.email.toLowerCase());
      }
      this._cacheWarmed = true;
      log.info({ event: 'domain_cache_warmed', known: this._knownDomains.size });
    } catch (err) {
      log.warn({ event: 'domain_cache_warn', error: err.message });
    }
  }

  async _handleTask(task) {
    if (task.command === 'shutdown' && (!task.target_node_id || task.target_node_id === this.nodeId)) {
      await this.stop('commander_shutdown');
    } else if (task.command === 'update_config') {
      this.config[task.key] = task.value;
    }
  }

  async _scrapeAndStore() {
    const freshLeads = [];

    for (const source of this.sources) {
      try {
        let leads = [];
        switch (source) {
          case 'google_scrape':   leads = await this._googleDiscoverLeads(); break;
          case 'web_scrape':      leads = await this._googleDiscoverLeads(); break;
          case 'yellowpages':     leads = await this._yellowPagesDiscover(); break;
          case 'hunter_discover': leads = await this._hunterDiscover();      break;
          case 'apollo':          leads = await this._scrapeApollo();       break;
          case 'hunter':          leads = await this._scrapeHunter();       break;
          case 'reddit':          leads = await this._scrapeReddit();       break;
          case 'linkedin':        leads = await this._scrapeLinkedIn();     break;
        }
        freshLeads.push(...leads);
        log.debug({ event: 'source_scraped', source, found: leads.length });
      } catch (err) {
        log.error({ event: 'scrape_error', source, error: err.message });
        this.increment('errors');
      }
      await sleep(2000); // polite gap between sources
    }

    const deduped = this._deduplicateLocally(freshLeads);
    let stored = 0;
    for (const lead of deduped) {
      const dom = (lead.website ?? '').toLowerCase().replace(/^www\./, '');
      const em  = (lead.email   ?? '').toLowerCase();
      if (dom && this._knownDomains.has(dom)) continue;
      if (em  && this._knownDomains.has(em))  continue;

      await this.memory.upsertLead({ ...lead, source_node: this.nodeId });
      if (dom) this._knownDomains.add(dom);
      if (em)  this._knownDomains.add(em);
      stored++;
    }

    if (stored > 0) {
      this.increment('leads_found', stored);
      await this.memory.writeMetric(this.nodeId, this.nodeType, 'leads_found', stored);
      log.info({ event: 'leads_stored', count: stored, node_id: this.nodeId });
    }
  }

  // ------------------------------------------------------------------ //
  //  SCRAPERS PER SOURCE                                                 //
  // ------------------------------------------------------------------ //

  /** Ensure shared Puppeteer browser is running */
  async _ensureBrowser() {
    if (!this._browser || !this._browser.isConnected()) {
      this._browser = await puppeteer.launch({
        headless: 'new',
        args: [
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--disable-dev-shm-usage',
          '--disable-gpu',
          '--window-size=1280,800',
        ],
      });
    }
    return this._browser;
  }

  /** Close browser when node stops */
  async stop(reason) {
    if (this._browser) {
      try { await this._browser.close(); } catch {}
      this._browser = null;
    }
    return super.stop(reason);
  }

  // ------------------------------------------------------------------ //
  //  GOOGLE SEARCH  →  WEBSITE SCRAPE  →  CHATBOT DETECTION              //
  // ------------------------------------------------------------------ //

  /**
   * Primary free pipeline:
   * 1. Google Search "[industry] in [city] [state]" via Puppeteer stealth
   * 2. Extract business website URLs from organic results
   * 3. Visit each site: scrape emails, phone, detect chatbot
   * 4. Skip businesses that already have a chatbot
   * 5. Return qualified leads (no chatbot = hot prospect)
   */
  async _googleDiscoverLeads() {
    const leads = [];
    const industries = this.targetIndustries;
    const locations  = this.targetLocations;

    for (const loc of locations) {
      // Enforce daily cap per city
      const cityKey = loc.toLowerCase().trim();
      const todayCount = this._scrapedToday.get(cityKey) ?? 0;
      if (todayCount >= this.dailyCapPerCity) {
        log.info({ event: 'daily_cap_reached', city: cityKey, cap: this.dailyCapPerCity });
        continue;
      }
      const remaining = this.dailyCapPerCity - todayCount;
      const cityLeads = []; // per-city counter so cap doesn't bleed across cities

      for (const industry of industries) {
        if (cityLeads.length >= remaining) break;

        // Run multiple query phrasings per industry to maximise unique local results
        const cityName  = loc.split(',')[0].trim();
        const statePart = loc.split(',').pop().trim();
        const queries   = [
          `${industry} in ${loc}`,
          `${industry} ${cityName} ${statePart}`,
          `local ${industry} ${cityName}`,
          `best ${industry} near ${cityName} ${statePart}`,
        ];

        for (const query of queries) {
          if (cityLeads.length >= remaining) break;
        try {
          const sites = await this._searchBusinesses(query);
          log.info({ event: 'bing_search', query, results: sites.length });

          for (const site of sites) {
            if (cityLeads.length >= remaining) break;
            const cleanDom = site.domain.toLowerCase().replace(/^www\./, '');
            if (this._scrapedDomains.has(cleanDom)) continue;
            if (this._knownDomains.has(cleanDom)) {
              log.debug({ event: 'skip_known', domain: cleanDom });
              continue;
            }
            this._scrapedDomains.add(cleanDom);

            try {
              const siteData = await this._scrapeWebsite(site.url);

              if (siteData.hasChatbot) {
                log.debug({ event: 'chatbot_detected', domain: site.domain, chatbot: siteData.chatbotName });
                continue; // Skip — they already have a chat solution
              }

              // Extract whatever contact info we found
              if (siteData.emails.length === 0 && !siteData.phone) {
                log.debug({ event: 'no_contact_info', domain: site.domain });
                continue;
              }

              // Reject sites that explicitly mention a DIFFERENT state than the target.
              // Most local business sites don't mention their city in the meta description,
              // so a hard "must contain city" check blocks everything. Instead we only
              // skip when the site clearly says it's somewhere else (e.g., "Serving Asheville, NC"
              // showing up for an Atlanta, GA search).
              // Reject sites that explicitly reference a different US state than the target.
              // Checks both abbreviations (OH) and full names (Ohio).
              const targetStateAbbr = loc.split(',').pop().trim().toLowerCase(); // e.g. "ga"
              const STATE_MAP = {
                'alabama':'al','alaska':'ak','arizona':'az','arkansas':'ar','california':'ca',
                'colorado':'co','connecticut':'ct','delaware':'de','florida':'fl','georgia':'ga',
                'hawaii':'hi','idaho':'id','illinois':'il','indiana':'in','iowa':'ia','kansas':'ks',
                'kentucky':'ky','louisiana':'la','maine':'me','maryland':'md','massachusetts':'ma',
                'michigan':'mi','minnesota':'mn','mississippi':'ms','missouri':'mo','montana':'mt',
                'nebraska':'ne','nevada':'nv','new hampshire':'nh','new jersey':'nj',
                'new mexico':'nm','new york':'ny','north carolina':'nc','north dakota':'nd',
                'ohio':'oh','oklahoma':'ok','oregon':'or','pennsylvania':'pa','rhode island':'ri',
                'south carolina':'sc','south dakota':'sd','tennessee':'tn','texas':'tx','utah':'ut',
                'vermont':'vt','virginia':'va','washington':'wa','west virginia':'wv',
                'wisconsin':'wi','wyoming':'wy',
              };
              const STATE_ABBRS = new Set(Object.values(STATE_MAP));
              const siteText    = ((siteData.tagline ?? '') + ' ' + (siteData.siteName ?? ''));
              const siteTextLow = siteText.toLowerCase();

              const foundStates = new Set();
              // Match abbreviated states (e.g. ", OH" or " OH ")
              for (const m of siteText.matchAll(/\b([A-Z]{2})\b/g)) {
                if (STATE_ABBRS.has(m[1].toLowerCase())) foundStates.add(m[1].toLowerCase());
              }
              // Match full state names (e.g. "Columbus, Ohio")
              for (const [name, abbr] of Object.entries(STATE_MAP)) {
                if (siteTextLow.includes(name)) foundStates.add(abbr);
              }

              if (foundStates.size > 0 && !foundStates.has(targetStateAbbr)) {
                log.debug({ event: 'location_mismatch', domain: site.domain, expected: loc, found: [...foundStates] });
                continue;
              }

              const leadBase = {
                name:               siteData.ownerName,
                title:              null,
                company:            siteData.siteName || site.title || null,
                website:            site.domain,
                phone:              siteData.phone,
                industry,
                location:           loc,
                has_chatbot:        false,
                site_tagline:       siteData.tagline || null,
                business_hours:     siteData.businessHours     || null,
                has_after_hours_gap: siteData.hasAfterHoursGap ?? true,
                has_contact_form:   siteData.hasContactForm    ?? false,
                site_platform:      siteData.sitePlatform      || 'custom',
                confidence:         null,
                verified:           null,
                source:             'web_scrape',
                linkedin_url:       null,
              };

              // Cap at 2 emails per domain — prevents news/directory sites from
              // flooding the list with dozens of staff addresses
              for (const email of siteData.emails.slice(0, 2)) {
                cityLeads.push({ ...leadBase, email });
              }

              // If we found a phone but no email, still store it
              if (siteData.emails.length === 0 && siteData.phone) {
                cityLeads.push({ ...leadBase, email: null });
              }
            } catch (err) {
              log.debug({ event: 'site_scrape_error', domain: site.domain, error: err.message });
            }

            await sleep(1500 + Math.random() * 2000); // Random delay between sites
          }
        } catch (err) {
          log.warn({ event: 'google_search_error', query, error: err.message });
        }

        await sleep(2000 + Math.random() * 2000); // Gap between query variations
        } // end query variations loop
        await sleep(1000); // Brief pause between industries
      }

      // Update daily counter and merge city leads into global array
      this._scrapedToday.set(cityKey, todayCount + cityLeads.length);
      leads.push(...cityLeads);
    }

    log.info({ event: 'web_scrape_complete', leads_found: leads.length });
    return leads;
  }

  /**
   * Perform a Bing search and return business website URLs.
   * Bing is much friendlier to Puppeteer than Google (no CAPTCHA).
   * Bing wraps result URLs in bing.com/ck/a redirect — we decode them.
   */
  async _searchBusinesses(query) {
    const browser = await this._ensureBrowser();
    const page = await browser.newPage();
    const results = [];

    // Directory / aggregator / national brand domains to skip — we want LOCAL business sites only
    const skipDomains = [
      // Directories & aggregators
      'yelp.com', 'yellowpages.com', 'bbb.org', 'healthgrades.com', 'zocdoc.com',
      'webmd.com', 'vitals.com', 'npiprofile.com', 'ratemds.com', 'angi.com',
      'thumbtack.com', 'nextdoor.com', 'tripadvisor.com', 'mapquest.com',
      'superpages.com', 'manta.com', 'chamberofcommerce.com', 'expertise.com',
      'bark.com', 'homeadvisor.com', 'houzz.com', 'thumbtack.com', 'porch.com',
      'angieslist.com', 'checkbook.org', 'homelight.com',
      // Social & search
      'google.com', 'bing.com', 'microsoft.com', 'facebook.com', 'youtube.com',
      'wikipedia.org', 'linkedin.com', 'twitter.com', 'x.com', 'instagram.com',
      'tiktok.com', 'pinterest.com', 'reddit.com',
      // News & media — these rank for industry queries but are NOT prospects
      'reuters.com', 'cnbc.com', 'bloomberg.com', 'forbes.com', 'wsj.com',
      'nytimes.com', 'washingtonpost.com', 'businessinsider.com', 'fortune.com',
      'inc.com', 'entrepreneur.com', 'foxnews.com', 'cnn.com', 'nbcnews.com',
      'apnews.com', 'axios.com', 'thehill.com', 'politico.com', 'usatoday.com',
      'marketwatch.com', 'investopedia.com', 'nerdwallet.com', 'bankrate.com',
      'kiplinger.com', 'motleyfool.com', 'seeking alpha.com',
      // National insurance & healthcare chains
      'deltadental.com', 'cigna.com', 'aetna.com', 'anthem.com', 'unitedhealthcare.com',
      'humana.com', 'bluecrossblue shield.com', 'bcbs.com', 'metlife.com',
      'mutualofomaha.com', 'guardianlife.com', 'principal.com',
      // National real estate brands
      'zillow.com', 'realtor.com', 'redfin.com', 'trulia.com', 'century21.com',
      'kw.com', 'kellerwilliams.com', 're/max.com', 'remax.com', 'coldwellbanker.com',
      'compass.com', 'sothebysrealty.com', 'bhhs.com',
      // National veterinary / pet chains
      'banfield.com', 'vca.com', 'bluepearlvet.com', 'petsmart.com', 'petco.com',
      'animalhumanesociety.org', 'aspca.org', 'humanesociety.org',
      // National dental / healthcare chains
      'aspen dental', 'aspendental.com', 'heartland dental', 'heartlanddental.com',
      'dentalworks.com', 'oralcare.com',
      // National auto chains
      'autotrader.com', 'cars.com', 'carmax.com', 'carvana.com', 'dealerSocket.com',
      // National legal / financial directories
      'avvo.com', 'findlaw.com', 'justia.com', 'lawyers.com', 'martindale.com',
      'smartasset.com', 'wealthmanagement.com', 'advisoryhq.com',
      // Job boards / HR sites that show up in searches
      'indeed.com', 'glassdoor.com', 'ziprecruiter.com', 'monster.com', 'salary.com',
      // Misc platforms that are not business sites
      'irs.gov', 'usa.gov', 'state.gov', 'medicare.gov', 'hhs.gov',
      'shopify.com', 'wix.com', 'squarespace.com', 'wordpress.com', 'godaddy.com',
      'amazon.com', 'ebay.com', 'etsy.com', 'walmart.com', 'target.com',
    ];

    try {
      await page.setViewport({ width: 1280, height: 800 });
      await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-US,en;q=0.9' });

      const searchUrl = `https://www.bing.com/search?q=${encodeURIComponent(query)}&count=30`;
      await page.goto(searchUrl, { waitUntil: 'networkidle2', timeout: 25_000 });

      // Extract organic result URLs from Bing's li.b_algo containers
      const rawResults = await page.evaluate(() => {
        const items = [];
        document.querySelectorAll('li.b_algo').forEach(el => {
          const a = el.querySelector('a[href]');
          if (a && a.href) {
            items.push({ url: a.href, title: a.textContent.trim() });
          }
        });
        return items;
      });

      // Decode Bing redirect URLs and filter to real business sites
      const seen = new Set();
      for (const r of rawResults) {
        const realUrl = this._decodeBingUrl(r.url);
        try {
          const domain = new URL(realUrl).hostname.replace(/^www\./, '');
          const isGov  = domain.endsWith('.gov') || domain.endsWith('.mil') || domain.endsWith('.edu');
          if (!isGov && !seen.has(domain) && !skipDomains.some(s => domain.includes(s))) {
            seen.add(domain);
            results.push({ url: realUrl, title: r.title, domain });
          }
        } catch {}
      }
    } catch (err) {
      log.warn({ event: 'bing_search_error', query, error: err.message });
    } finally {
      await page.close().catch(() => {});
    }

    return results.slice(0, 15); // Max 15 unique business sites per query
  }

  /** Decode Bing redirect URL: bing.com/ck/a?...u=a1<base64>... → real URL */
  _decodeBingUrl(bingUrl) {
    try {
      const uParam = new URL(bingUrl).searchParams.get('u');
      if (uParam && uParam.startsWith('a1')) {
        return Buffer.from(uParam.slice(2), 'base64').toString('utf8');
      }
    } catch {}
    return bingUrl; // fallback: return as-is
  }

  /**
   * Visit a business website with Puppeteer.
   * Scrapes: emails, phone numbers, owner/team names.
   * Detects: chat widget presence.
   */
  async _scrapeWebsite(url) {
    const browser = await this._ensureBrowser();
    const page = await browser.newPage();

    const result = {
      emails:           [],
      phone:            null,
      ownerName:        null,
      hasChatbot:       false,
      chatbotName:      null,
      tagline:          null,
      siteName:         null,
      businessHours:    null,
      hasAfterHoursGap: true,
      hasContactForm:   false,
      sitePlatform:     'custom',
    };

    try {
      // Block heavy resources to speed up scraping
      await page.setRequestInterception(true);
      page.on('request', req => {
        const type = req.resourceType();
        if (['image', 'media', 'font', 'stylesheet'].includes(type)) {
          req.abort();
        } else {
          req.continue();
        }
      });

      await page.setViewport({ width: 1280, height: 800 });
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 15_000 });

      // Wait a bit for dynamic chat widgets to inject
      await sleep(2000);

      // Skip Cloudflare / bot-detection challenge pages
      const pageTitle = await page.title().catch(() => '');
      const CHALLENGE_PHRASES = ['just a moment', 'attention required', 'security check', 'ddos protection', 'please wait', 'checking your browser'];
      if (CHALLENGE_PHRASES.some(p => pageTitle.toLowerCase().includes(p))) {
        log.debug({ event: 'cloudflare_blocked', url });
        return result;
      }

      // Scrape the main page
      const homeData = await this._extractPageData(page);
      Object.assign(result, this._mergePageData(result, homeData));

      // Try /contact page for more emails/phone
      const contactUrl = new URL(url);
      for (const path of ['/contact', '/contact-us', '/about', '/about-us']) {
        if (result.emails.length > 0 && result.phone) break; // already have what we need
        try {
          contactUrl.pathname = path;
          await page.goto(contactUrl.href, { waitUntil: 'domcontentloaded', timeout: 10_000 });
          await sleep(1000);
          const pageData = await this._extractPageData(page);
          Object.assign(result, this._mergePageData(result, pageData));
        } catch {}
      }
    } catch (err) {
      log.debug({ event: 'website_scrape_error', url, error: err.message });
    } finally {
      await page.close().catch(() => {});
    }

    // Deduplicate and strip generic/technical prefixes — we want real humans
    const JUNK_PREFIXES = [
      'dev', 'webmaster', 'admin', 'noreply', 'no-reply', 'support',
      'help', 'hello', 'team', 'media', 'press', 'marketing', 'sales',
      'billing', 'accounts', 'hr', 'careers', 'jobs', 'legal', 'privacy',
      'abuse', 'spam', 'postmaster', 'hostmaster', 'newsletter',
      'editor', 'news', 'general', 'enquiries', 'enquiry',
    ];
    result.emails = [...new Set(result.emails)].filter(email => {
      const prefix = email.split('@')[0].toLowerCase().replace(/[^a-z]/g, '');
      return !JUNK_PREFIXES.includes(prefix);
    });
    return result;
  }

  /**
   * Extract emails, phones, chatbot signals, and owner name from current page.
   * Runs inside Puppeteer page context.
   */
  async _extractPageData(page) {
    return page.evaluate((signatures) => {
      const html = document.documentElement.outerHTML.toLowerCase();
      const bodyText = document.body?.innerText ?? '';

      // --- Emails ---
      const emailRegex = /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/g;
      const rawEmails = (bodyText.match(emailRegex) || [])
        .concat((html.match(/mailto:([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})/gi) || [])
          .map(m => m.replace(/^mailto:/i, '')));
      // Filter out junk
      const emails = [...new Set(rawEmails)].filter(e => {
        const lower = e.toLowerCase();
        return !lower.includes('example.com') && !lower.includes('sentry')
            && !lower.includes('wixpress') && !lower.includes('.png')
            && !lower.includes('.jpg') && !lower.includes('.css')
            && !lower.endsWith('.js') && lower.length < 80;
      });

      // --- Phone numbers ---
      const phoneRegex = /(?:\+1[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}/g;
      const phones = bodyText.match(phoneRegex) || [];
      const phone = phones.length > 0 ? phones[0].replace(/[^\d+]/g, '') : null;

      // --- Chatbot detection ---
      let hasChatbot = false;
      let chatbotName = null;

      // Check script srcs
      const scripts = [...document.querySelectorAll('script[src]')];
      for (const script of scripts) {
        const src = script.src.toLowerCase();
        for (const sig of signatures) {
          if (src.includes(sig)) {
            hasChatbot = true;
            chatbotName = sig;
            break;
          }
        }
        if (hasChatbot) break;
      }

      // Check inline scripts and HTML for chat widget markers
      if (!hasChatbot) {
        for (const sig of signatures) {
          if (html.includes(sig)) {
            hasChatbot = true;
            chatbotName = sig;
            break;
          }
        }
      }

      // Check for common chat widget DOM elements
      if (!hasChatbot) {
        const chatSelectors = [
          '#intercom-container', '#drift-widget', '.tidio-chat',
          '#livechat-compact-container', '#hubspot-messages-iframe-container',
          '#crisp-chatbox', '.tawk-min-container', '#olark-wrapper',
          '[class*="chat-widget"]', '[class*="chatwidget"]',
          '[id*="chat-widget"]', '[id*="chatwidget"]',
          'iframe[src*="livechat"]', 'iframe[src*="tawk"]',
          'iframe[src*="tidio"]', 'iframe[src*="drift"]',
          'iframe[title*="chat"]', 'iframe[title*="Chat"]',
        ];
        for (const sel of chatSelectors) {
          if (document.querySelector(sel)) {
            hasChatbot = true;
            chatbotName = sel;
            break;
          }
        }
      }

      // --- Owner / key person name ---
      let ownerName = null;
      const ownerPatterns = [
        /(?:owner|founder|ceo|president|principal|dr\.?|dds|dmd|pa-c|np|attorney|realtor|broker)[:\s,]+([A-Z][a-z]+(?:\s[A-Z]\.?)?\s[A-Z][a-z]+)/i,
        /([A-Z][a-z]+(?:\s[A-Z]\.?)?\s[A-Z][a-z]+)[,\s]+(?:owner|founder|ceo|president|dds|dmd|attorney|realtor|broker)/i,
        /(?:meet\s+(?:dr\.?\s+)?|about\s+(?:dr\.?\s+)?|hi,?\s+i(?:'m| am)\s+)([A-Z][a-z]+ [A-Z][a-z]+)/i,
        /(?:^|\n)([A-Z][a-z]+ [A-Z][a-z]+)\s*\n\s*(?:owner|founder|ceo|dds|dmd|attorney)/im,
      ];
      for (const pat of ownerPatterns) {
        const match = bodyText.match(pat);
        if (match) { ownerName = match[1].trim(); break; }
      }
      if (!ownerName) {
        const headings = [...document.querySelectorAll('h1, h2')]
          .map(h => h.innerText?.trim()).filter(Boolean);
        for (const h of headings) {
          const m = h.match(/^(?:dr\.?\s+)?([A-Z][a-z]+ [A-Z][a-z]+)(?:,\s*(?:dds|dmd|pa|np|esq|cpa|attorney|owner))?$/i);
          if (m) { ownerName = m[1].trim(); break; }
        }
      }

      // --- Business hours ---
      let businessHours = null;
      let hasAfterHoursGap = true;
      const hoursMatch = bodyText.match(
        /(?:hours?|open)[:\s]*([^\n]{5,80}(?:am|pm|closed)[^\n]{0,60})/i
      );
      if (hoursMatch) {
        businessHours = hoursMatch[1].trim().slice(0, 120);
        const h = businessHours.toLowerCase();
        hasAfterHoursGap = !(/sat|sun/.test(h)) || !(/[6-9]\s*pm|10\s*pm|11\s*pm/.test(h));
      }

      // --- Contact form + platform ---
      const hasContactForm = !!document.querySelector(
        'form[action], form[id*="contact"], form[class*="contact"], form[id*="inquiry"], form[class*="inquiry"]'
      );
      let sitePlatform = 'custom';
      if (html.includes('wp-content') || html.includes('wordpress')) sitePlatform = 'wordpress';
      else if (html.includes('wix.com') || html.includes('wixsite')) sitePlatform = 'wix';
      else if (html.includes('squarespace')) sitePlatform = 'squarespace';
      else if (html.includes('shopify')) sitePlatform = 'shopify';
      else if (html.includes('webflow')) sitePlatform = 'webflow';
      else if (html.includes('godaddy')) sitePlatform = 'godaddy';

      // --- Meta ---
      const metaDesc = document.querySelector('meta[name="description"]')?.getAttribute('content')
        ?? document.querySelector('meta[property="og:description"]')?.getAttribute('content')
        ?? null;
      const siteName = document.querySelector('meta[property="og:site_name"]')?.getAttribute('content')
        ?? document.title ?? null;
      const tagline = (metaDesc ?? '').slice(0, 200).trim() || null;

      return {
        emails, phone, hasChatbot, chatbotName, ownerName, tagline, siteName,
        businessHours, hasAfterHoursGap, hasContactForm, sitePlatform,
      };
    }, CHATBOT_SIGNATURES);
  }

  /** Merge page data into running result, keeping best values */
  _mergePageData(existing, pageData) {
    return {
      emails:           [...(existing.emails || []), ...(pageData.emails || [])],
      phone:            existing.phone            || pageData.phone,
      ownerName:        existing.ownerName        || pageData.ownerName,
      hasChatbot:       existing.hasChatbot       || pageData.hasChatbot,
      chatbotName:      existing.chatbotName      || pageData.chatbotName,
      tagline:          existing.tagline          || pageData.tagline          || null,
      siteName:         existing.siteName         || pageData.siteName         || null,
      businessHours:    existing.businessHours    || pageData.businessHours    || null,
      hasAfterHoursGap: existing.hasAfterHoursGap ?? pageData.hasAfterHoursGap ?? true,
      hasContactForm:   existing.hasContactForm   ?? pageData.hasContactForm   ?? false,
      sitePlatform:     existing.sitePlatform     || pageData.sitePlatform     || 'custom',
    };
  }

  // ------------------------------------------------------------------ //
  //  YELLOW PAGES  →  LOCAL DIRECTORY SCRAPE  (free, no API)             //
  // ------------------------------------------------------------------ //

  /**
   * Scrapes YellowPages.com search results for each industry/city combo.
   * YP returns 30 actual local businesses per page with phone + website URL.
   * Far more reliable than Bing organic for small-city local business discovery.
   */
  async _yellowPagesDiscover() {
    const leads     = [];
    const browser   = await this._ensureBrowser();
    const YP_INDUSTRY_MAP = {
      'Dental Offices':               'dentists',
      'Medical Clinics':              'medical-clinics',
      'Real Estate Agencies':         'real-estate-agents',
      'Law Firms':                    'attorneys',
      'HVAC & Plumbing Contractors':  'hvac',
      'Auto Repair Shops':            'auto-repair',
      'Veterinary Clinics':           'veterinarians',
      'Financial Advisors':           'financial-planning-consultants',
      'Insurance Agencies':           'insurance',
      'Chiropractic Offices':         'chiropractors',
    };

    for (const loc of this.targetLocations) {
      const cityKey    = loc.toLowerCase().trim();
      const todayCount = this._scrapedToday.get(cityKey) ?? 0;
      if (todayCount >= this.dailyCapPerCity) continue;
      const remaining  = this.dailyCapPerCity - todayCount;
      const cityLeads  = [];

      const citySlug  = loc.split(',')[0].trim().toLowerCase().replace(/\s+/g, '-');
      const stateSlug = loc.split(',').pop().trim().toLowerCase();

      for (const industry of this.targetIndustries) {
        if (cityLeads.length >= remaining) break;
        const ypCategory = YP_INDUSTRY_MAP[industry] ?? industry.toLowerCase().replace(/\s+/g, '-');
        const url = `https://www.yellowpages.com/${citySlug}-${stateSlug}/${ypCategory}`;

        let page;
        try {
          page = await browser.newPage();
          await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
          await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 20_000 });
          await sleep(2000);

          const pageTitle = await page.title().catch(() => '');
          const CHALLENGE_PHRASES = ['just a moment', 'attention required', 'security check', 'ddos protection'];
          if (CHALLENGE_PHRASES.some(p => pageTitle.toLowerCase().includes(p))) {
            log.debug({ event: 'yp_blocked', url });
            await page.close();
            continue;
          }

          // Extract listings from YP results page
          const listings = await page.evaluate(() => {
            const results = [];
            document.querySelectorAll('.result').forEach(el => {
              const name    = el.querySelector('.business-name')?.textContent?.trim() ?? null;
              const phone   = el.querySelector('.phones')?.textContent?.trim() ?? null;
              const website = el.querySelector('a.track-visit-website')?.href
                           ?? el.querySelector('[class*="website"]')?.href
                           ?? null;
              const street  = el.querySelector('.street-address')?.textContent?.trim() ?? null;
              const locality = el.querySelector('.locality')?.textContent?.trim() ?? null;
              if (name) results.push({ name, phone, website, street, locality });
            });
            return results;
          });

          log.info({ event: 'yp_search', url, results: listings.length });

          for (const listing of listings) {
            if (cityLeads.length >= remaining) break;
            if (!listing.website) continue;

            let domain;
            try { domain = new URL(listing.website).hostname.replace(/^www\./, ''); } catch { continue; }
            if (this._scrapedDomains.has(domain)) continue;
            this._scrapedDomains.add(domain);

            // Skip national chains / junk domains (reuse same skip list)
            const SKIP = ['yelp.com','yellowpages.com','bbb.org','facebook.com','healthgrades.com',
              'webmd.com','zocdoc.com','google.com','linkedin.com','insurance.com',
              'progressive.com','statefarm.com','allstate.com','nationwide.com',
              'banfield.com','vca.com','petsmart.com','petco.com'];
            if (SKIP.some(s => domain.includes(s))) continue;

            // Skip franchise agents — they aren't independent businesses
            const FRANCHISE_BRANDS = [
              'state farm', 'allstate', 'geico', 'primerica', 'farmers insurance',
              'nationwide', 'liberty mutual', 'country financial', 'edward jones',
              'ameriprise', 'raymond james', 'nw mutual', 'northwestern mutual',
              'keller williams', 're/max', 'remax', 'coldwell banker', 'century 21',
              'exp realty', 'compass real estate',
            ];
            const listingLower = listing.name.toLowerCase();
            if (FRANCHISE_BRANDS.some(b => listingLower.includes(b))) continue;

            // Scrape the actual business site for email + tagline
            let siteData = { emails: [], phone: listing.phone, ownerName: null, tagline: null, siteName: listing.name, hasChatbot: false, businessHours: null, hasAfterHoursGap: true, hasContactForm: false, sitePlatform: 'custom' };
            try {
              siteData = await this._scrapeWebsite(listing.website);
              siteData.phone    = siteData.phone    ?? listing.phone;
              siteData.siteName = siteData.siteName ?? listing.name;
            } catch {}

            if (siteData.hasChatbot) {
              log.debug({ event: 'chatbot_found', domain });
              continue;
            }

            const leadBase = {
              name:         siteData.ownerName ?? null,
              title:        null,
              company:      listing.name,
              website:      domain,
              phone:        siteData.phone ?? listing.phone,
              industry,
              location:     loc,
              has_chatbot:         false,
              site_tagline:        siteData.tagline         ?? null,
              business_hours:      siteData.businessHours   ?? null,
              has_after_hours_gap: siteData.hasAfterHoursGap ?? true,
              has_contact_form:    siteData.hasContactForm   ?? false,
              site_platform:       siteData.sitePlatform     || 'custom',
              confidence:          null,
              verified:            null,
              source:              'yellowpages',
              linkedin_url:        null,
            };

            if (siteData.emails.length > 0) {
              for (const email of siteData.emails.slice(0, 2)) {
                cityLeads.push({ ...leadBase, email });
              }
            } else {
              // Keep phone-only leads — still useful for the database
              cityLeads.push({ ...leadBase, email: null });
            }

            await sleep(1500 + Math.random() * 1500);
          }

          await page.close();
        } catch (err) {
          log.warn({ event: 'yp_error', url, error: err.message });
          try { await page?.close(); } catch {}
        }

        await sleep(2000 + Math.random() * 2000);
      }

      this._scrapedToday.set(cityKey, todayCount + cityLeads.length);
      leads.push(...cityLeads);
    }

    log.info({ event: 'yp_scrape_complete', leads_found: leads.length });
    return leads;
  }

  // ------------------------------------------------------------------ //
  //  HUNTER DISCOVER  →  DOMAIN SEARCH  (secondary, credit-based)        //
  // ------------------------------------------------------------------ //
  async _hunterDiscover() {
    if (!this.hunterApiKey) return [];

    // Build location filters from profile ICP locations
    const locations = this.hunterLocations.length > 0
      ? this.hunterLocations
      : this.targetLocations.map(loc => {
          // Parse "City, State" or "State" from profile target_locations
          const parts = loc.split(',').map(s => s.trim());
          if (parts.length >= 2) return { country: 'US', state: this._stateCode(parts[1]), city: parts[0] };
          if (parts[0] === 'United States') return { country: 'US' };
          return { country: 'US', state: this._stateCode(parts[0]) };
        });

    const body = {
      headcount: this.hunterHeadcount,
    };
    if (locations.length) body.headquarters_location = { include: locations };
    if (this.hunterIndustries.length) body.industry = { include: this.hunterIndustries };

    const resp = await axios.post(
      `https://api.hunter.io/v2/discover?api_key=${this.hunterApiKey}`,
      body,
      { headers: { 'Content-Type': 'application/json' }, timeout: 20_000 },
    );

    const companies = resp.data?.data ?? [];
    // Only process companies that have personal emails and we haven't searched yet
    const targets = companies
      .filter(c => (c.emails_count?.personal ?? 0) > 0 && !this._searchedDomains.has(c.domain))
      .slice(0, Math.min(10, this.leadsPerCycle));   // Cap domain searches per cycle (10 credits max)

    const leads = [];
    for (const co of targets) {
      this._searchedDomains.add(co.domain);
      try {
        const dsResp = await axios.get('https://api.hunter.io/v2/domain-search', {
          params: {
            domain:   co.domain,
            api_key:  this.hunterApiKey,
            limit:    10,
            type:     'personal',
            seniority: 'executive,senior',
          },
          timeout: 10_000,
        });
        for (const e of dsResp.data?.data?.emails ?? []) {
          if (!e.value) continue;
          leads.push({
            email:      e.value,
            name:       ((e.first_name ?? '') + ' ' + (e.last_name ?? '')).trim() || null,
            title:      e.position ?? null,
            company:    co.organization ?? null,
            website:    co.domain,
            industry:   null,
            location:   null,
            confidence: e.confidence,
            verified:   e.verification?.status ?? null,
            source:     'hunter_discover',
            linkedin_url: e.linkedin ?? null,
            phone:      e.phone_number ?? null,
          });
        }
        await sleep(300);   // respect Hunter rate limits
      } catch (err) {
        log.warn({ event: 'hunter_domain_search_error', domain: co.domain, error: err.message });
      }
    }

    log.info({ event: 'hunter_discover_complete', companies_found: companies.length, domains_searched: targets.length, leads_extracted: leads.length });
    return leads;
  }

  /**
   * Apollo.io people search (requires paid plan for API access).
   * Key must go in X-Api-Key header (not body).
   */
  async _scrapeApollo() {
    if (!this.apolloApiKey) return [];
    try {
      const resp = await axios.post(
        'https://api.apollo.io/v1/mixed_people/search',
        {
          person_titles:    this.targetTitles,
          person_locations: this.targetLocations,
          organization_num_employees_ranges: [
            `${this.minCompanySize},${this.maxCompanySize}`,
          ],
          q_organization_industries: this.targetIndustries,
          per_page: Math.min(this.leadsPerCycle, 100),
        },
        {
          timeout: 20_000,
          headers: {
            'X-Api-Key':    this.apolloApiKey,
            'Content-Type': 'application/json',
          },
        },
      );

      return (resp.data.people ?? []).map(p => ({
        name:         `${p.first_name ?? ''} ${p.last_name ?? ''}`.trim(),
        email:        p.email,
        phone:        p.phone_numbers?.[0]?.sanitized_number,
        title:        p.title,
        company:      p.organization?.name,
        linkedin_url: p.linkedin_url,
        industry:     p.organization?.industry,
        location:     p.city,
        source:       'apollo',
      }));
    } catch (err) {
      // Apollo free plan blocks API — log and move on
      if (err.response?.status === 403) {
        log.info({ event: 'apollo_api_blocked', reason: 'Free plan — API not accessible. Upgrade or rely on Hunter.' });
        return [];
      }
      throw err;
    }
  }

  async _scrapeHunter() {
    if (!this.hunterApiKey) return [];
    // Legacy single-domain search (used if a specific target_domain is set)
    const domain = this.config.target_domain;
    if (!domain) return [];

    const resp = await axios.get('https://api.hunter.io/v2/domain-search', {
      params: { domain, api_key: this.hunterApiKey, limit: 20, type: 'personal' },
      timeout: 10_000,
    });

    const company = resp.data.data?.organization;
    return (resp.data.data?.emails ?? []).map(e => ({
      email:   e.value,
      name:    `${e.first_name ?? ''} ${e.last_name ?? ''}`.trim(),
      title:   e.position,
      company,
      source:  'hunter',
    }));
  }

  async _scrapeReddit() {
    // Find leads from relevant subreddits (people asking for solutions)
    const subreddits = this.config.lead_subreddits ?? ['forhire', 'entrepreneur', 'smallbusiness'];
    const all = [];
    for (const sub of subreddits.slice(0, 2)) {
      try {
        const resp = await axios.get(`https://www.reddit.com/r/${sub}/new.json?limit=25`, {
          headers: { 'User-Agent': 'Swarm/1.0' },
          timeout: 10_000,
        });
        const posts = resp.data?.data?.children ?? [];
        for (const post of posts) {
          const d = post.data;
          if (d.author && d.author !== '[deleted]') {
            all.push({
              reddit_user: d.author,
              pain_point:  d.title,
              source:      `reddit:${sub}`,
              post_url:    `https://reddit.com${d.permalink}`,
            });
          }
        }
      } catch (err) {
        log.warn({ event: 'reddit_scrape_warn', sub, error: err.message });
      }
      await sleep(1500);
    }
    return all;
  }

  async _scrapeLinkedIn() {
    // LinkedIn requires official API — stub with enrichment fallback
    if (!this.config.linkedin_scraper_url) return [];
    try {
      const resp = await axios.get(this.config.linkedin_scraper_url, {
        params: {
          titles:     this.targetTitles.join(','),
          industries: this.targetIndustries.join(','),
          limit:      25,
          token:      this.config.linkedin_scraper_token,
        },
        timeout: 30_000,
      });
      return resp.data?.leads ?? [];
    } catch (err) {
      log.warn({ event: 'linkedin_scrape_warn', error: err.message });
      return [];
    }
  }

  // ------------------------------------------------------------------ //
  //  HELPERS                                                              //
  // ------------------------------------------------------------------ //

  /** Convert state name or abbreviation to 2-letter state code for Hunter API */
  _stateCode(input) {
    if (!input) return undefined;
    const s = input.trim();
    if (s.length === 2) return s.toUpperCase();
    const map = {
      'alabama':'AL','alaska':'AK','arizona':'AZ','arkansas':'AR','california':'CA',
      'colorado':'CO','connecticut':'CT','delaware':'DE','florida':'FL','georgia':'GA',
      'hawaii':'HI','idaho':'ID','illinois':'IL','indiana':'IN','iowa':'IA',
      'kansas':'KS','kentucky':'KY','louisiana':'LA','maine':'ME','maryland':'MD',
      'massachusetts':'MA','michigan':'MI','minnesota':'MN','mississippi':'MS',
      'missouri':'MO','montana':'MT','nebraska':'NE','nevada':'NV','new hampshire':'NH',
      'new jersey':'NJ','new mexico':'NM','new york':'NY','north carolina':'NC',
      'north dakota':'ND','ohio':'OH','oklahoma':'OK','oregon':'OR','pennsylvania':'PA',
      'rhode island':'RI','south carolina':'SC','south dakota':'SD','tennessee':'TN',
      'texas':'TX','utah':'UT','vermont':'VT','virginia':'VA','washington':'WA',
      'west virginia':'WV','wisconsin':'WI','wyoming':'WY',
    };
    return map[s.toLowerCase()] ?? s.toUpperCase().slice(0, 2);
  }

  // ------------------------------------------------------------------ //
  //  DEDUPLICATION                                                       //
  // ------------------------------------------------------------------ //

  _deduplicateLocally(leads) {
    const flat = leads.flat().filter(Boolean);
    const seen = new Set();
    return flat.filter(lead => {
      const key = (lead.email ?? lead.reddit_user ?? lead.website ?? '').toLowerCase();
      if (!key || seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }

  collectMetrics() {
    return {
      leads_found: this.getCounter('leads_found'),
      errors:      this.getCounter('errors'),
    };
  }

  getImprovementContext() {
    return {
      config:           this.config,
      target_titles:    this.targetTitles,
      target_industries: this.targetIndustries,
      sources:          this.sources,
      leads_per_cycle:  this.leadsPerCycle,
    };
  }
}
