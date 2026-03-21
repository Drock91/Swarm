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
import OpenAI from 'openai';
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
    this.openai          = new OpenAI();
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
    this.dailyCapPerCity  = config.daily_cap_per_city ?? 30;      // max businesses to scrape per city/day
    this._scrapedToday    = new Map();                             // city → count
    this._scrapedDomains  = new Set();                             // global dedup across cycles
    this._browser         = null;                                  // shared Puppeteer browser

    // Track which domains we've already searched via Hunter (to avoid burning search credits)
    this._searchedDomains = new Set();
  }

  async runCycle() {
    const tasks = await this.receiveTasks(10);
    for (const t of tasks) {
      await this._handleTask(t);
      await this.ackTask(t._receipt_handle);
    }
    await this._scrapeAndStore();
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

    const deduped = await this._deduplicate(freshLeads);
    let stored = 0;
    for (const lead of deduped) {
      await this.memory.upsertLead({ ...lead, source_node: this.nodeId });
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

      for (const industry of industries) {
        if (leads.length >= remaining) break;

        const query = `${industry} in ${loc}`;
        try {
          const sites = await this._searchBusinesses(query);
          log.info({ event: 'bing_search', query, results: sites.length });

          for (const site of sites) {
            if (leads.length >= remaining) break;
            if (this._scrapedDomains.has(site.domain)) continue;
            this._scrapedDomains.add(site.domain);

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

              for (const email of siteData.emails) {
                leads.push({
                  email,
                  name:        siteData.ownerName,
                  title:       null,
                  company:     site.title || null,
                  website:     site.domain,
                  phone:       siteData.phone,
                  industry,
                  location:    loc,
                  has_chatbot: false,
                  confidence:  null,
                  verified:    null,
                  source:      'web_scrape',
                  linkedin_url: null,
                });
              }

              // If we found a phone but no email, still store it
              if (siteData.emails.length === 0 && siteData.phone) {
                leads.push({
                  email:       null,
                  name:        siteData.ownerName,
                  title:       null,
                  company:     site.title || null,
                  website:     site.domain,
                  phone:       siteData.phone,
                  industry,
                  location:    loc,
                  has_chatbot: false,
                  confidence:  null,
                  verified:    null,
                  source:      'web_scrape',
                  linkedin_url: null,
                });
              }
            } catch (err) {
              log.debug({ event: 'site_scrape_error', domain: site.domain, error: err.message });
            }

            await sleep(1500 + Math.random() * 2000); // Random delay between sites
          }
        } catch (err) {
          log.warn({ event: 'google_search_error', query, error: err.message });
        }

        await sleep(3000 + Math.random() * 4000); // Gap between Google searches
      }

      // Update daily counter
      this._scrapedToday.set(cityKey, todayCount + leads.length);
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

    // Directory / aggregator domains to skip — we want actual business sites
    const skipDomains = [
      'yelp.com', 'yellowpages.com', 'bbb.org', 'healthgrades.com', 'zocdoc.com',
      'webmd.com', 'vitals.com', 'npiprofile.com', 'deltadental.com', 'cigna.com',
      'aetna.com', 'ratemds.com', 'google.com', 'bing.com', 'microsoft.com',
      'facebook.com', 'youtube.com', 'wikipedia.org', 'linkedin.com',
      'twitter.com', 'instagram.com', 'angi.com', 'thumbtack.com',
      'nextdoor.com', 'tripadvisor.com', 'mapquest.com',
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
          if (!seen.has(domain) && !skipDomains.some(s => domain.includes(s))) {
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
      emails:      [],
      phone:       null,
      ownerName:   null,
      hasChatbot:  false,
      chatbotName: null,
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

    // Deduplicate emails
    result.emails = [...new Set(result.emails)];
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

      // --- Owner / key person name (best-effort) ---
      let ownerName = null;
      const ownerPatterns = [
        /(?:owner|founder|ceo|president|principal|dr\.?|dds|dmd)[:\s,]+([A-Z][a-z]+ [A-Z][a-z]+)/i,
        /([A-Z][a-z]+ [A-Z][a-z]+)[,\s]+(?:owner|founder|ceo|president|dds|dmd)/i,
      ];
      for (const pat of ownerPatterns) {
        const match = bodyText.match(pat);
        if (match) { ownerName = match[1].trim(); break; }
      }

      return { emails, phone, hasChatbot, chatbotName, ownerName };
    }, CHATBOT_SIGNATURES);
  }

  /** Merge page data into running result, keeping best values */
  _mergePageData(existing, pageData) {
    return {
      emails:      [...(existing.emails || []), ...(pageData.emails || [])],
      phone:       existing.phone || pageData.phone,
      ownerName:   existing.ownerName || pageData.ownerName,
      hasChatbot:  existing.hasChatbot || pageData.hasChatbot,
      chatbotName: existing.chatbotName || pageData.chatbotName,
    };
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

  async _deduplicate(leads) {
    if (!leads.length) return [];
    const existing = await this.memory.getLeads(null, 5000);
    const knownEmails = new Set(existing.map(l => l.email).filter(Boolean));
    const knownReddit = new Set(existing.map(l => l.reddit_user).filter(Boolean));
    return leads.filter(l => {
      if (l.email && knownEmails.has(l.email)) return false;
      if (l.reddit_user && knownReddit.has(l.reddit_user)) return false;
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
