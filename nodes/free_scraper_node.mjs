/**
 * FreeScraperNode — 100% API-free lead generation pipeline.
 *
 * Sources (all Puppeteer stealth, zero paid APIs):
 *   - Yelp local search    → business list with website links
 *   - Google organic       → local business websites from search results
 *   - BBB local search     → vetted small businesses with contact info
 *   - Google Maps          → richest local business dataset
 *
 * Email extraction (3-layer waterfall per domain):
 *   1. Deep website scrape — home + contact + about + team + industry-specific pages
 *   2. RDAP/WHOIS registrant lookup
 *   3. Pattern generation + MX record validation
 *
 * Quality gates (drops a lead if it fails any):
 *   - Must have email OR phone
 *   - Domain must not be a national chain / directory / franchise
 *   - Site must not already have a chatbot
 *   - Domain must not be .gov / .edu / .mil
 *
 * Stealth:
 *   - puppeteer-extra-plugin-stealth
 *   - 15 rotating real Chrome user agents
 *   - Random realistic viewport sizes
 *   - Human-like jittered delays
 *   - Isolated browser contexts per source
 *   - Blocks images/fonts/stylesheets for speed
 */

import axios    from 'axios';
import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import { BaseNode } from '../core/base_node.mjs';
import { log }      from '../core/logger.mjs';
import dns          from 'dns/promises';

puppeteer.use(StealthPlugin());

const sleep  = ms  => new Promise(r => setTimeout(r, ms));
const jitter = (base, spread) => Math.floor(base + Math.random() * spread);

// ── Chatbot signatures ───────────────────────────────────────────────────────
const CHATBOT_SIGNATURES = [
  'intercom', 'drift.com', 'tidio.co', 'livechat', 'livechatinc',
  'zendesk', 'zopim', 'freshchat', 'freshdesk', 'crisp.chat',
  'tawk.to', 'olark', 'chatwoot', 'botpress', 'hubspot',
  'birdeye', 'podium', 'webchat', 'manychat', 'chatbot.com',
  'kommunicate', 'smartsupp', 'jivochat', 'purechat', 'chatra',
  'userlike', 'gorgias', 'helpcrunch', 'customerly', 'acquire.io',
  'smith.ai', 'chatfuel', 'landbot', 'collect.chat', 'activechat',
  'widget.js', 'chat-widget', 'chatwidget', 'live-chat',
];

// ── Rotating real Chrome user agents ────────────────────────────────────────
const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0',
  'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
];

// ── Industry → search terms per source ──────────────────────────────────────
const YELP_CATEGORIES = {
  'Dental Offices':              'dentists',
  'Medical Clinics':             'medicaldentists',
  'Real Estate Agencies':        'realestate',
  'Law Firms':                   'lawyers',
  'HVAC & Plumbing Contractors': 'hvac',
  'Auto Repair Shops':           'autorepair',
  'Veterinary Clinics':          'vets',
  'Financial Advisors':          'financialadvising',
  'Insurance Agencies':          'insurance',
  'Chiropractic Offices':        'chiropractors',
};

const BBB_TERMS = {
  'Dental Offices':              'dental office',
  'Medical Clinics':             'medical clinic',
  'Real Estate Agencies':        'real estate agency',
  'Law Firms':                   'law firm',
  'HVAC & Plumbing Contractors': 'hvac contractor',
  'Auto Repair Shops':           'auto repair',
  'Veterinary Clinics':          'veterinary clinic',
  'Financial Advisors':          'financial advisor',
  'Insurance Agencies':          'insurance agency',
  'Chiropractic Offices':        'chiropractic',
};

// Industry-specific sub-pages most likely to have staff/owner emails
const INDUSTRY_CONTACT_PATHS = {
  'Dental Offices':              ['/our-dentist', '/meet-the-doctor', '/our-team', '/staff', '/dentists', '/dr'],
  'Medical Clinics':             ['/our-providers', '/providers', '/doctors', '/our-doctors', '/physicians', '/staff'],
  'Real Estate Agencies':        ['/our-agents', '/agents', '/team', '/meet-our-team', '/realtors', '/roster'],
  'Law Firms':                   ['/our-attorneys', '/attorneys', '/lawyers', '/team', '/professionals', '/practice'],
  'HVAC & Plumbing Contractors': ['/about', '/team', '/staff', '/our-team'],
  'Auto Repair Shops':           ['/about', '/staff', '/our-team', '/meet-us'],
  'Veterinary Clinics':          ['/our-veterinarians', '/vets', '/our-team', '/staff', '/meet-the-doctor', '/doctors'],
  'Financial Advisors':          ['/our-advisors', '/advisors', '/team', '/about-us', '/professionals'],
  'Insurance Agencies':          ['/our-agents', '/agents', '/team', '/staff', '/meet-us'],
  'Chiropractic Offices':        ['/our-chiropractor', '/meet-the-doctor', '/our-team', '/staff', '/doctors'],
};

// Email prefixes that are not real humans — strip these
const JUNK_PREFIXES = new Set([
  'dev', 'webmaster', 'admin', 'noreply', 'no-reply', 'support',
  'help', 'team', 'media', 'press', 'billing', 'accounts',
  'hr', 'careers', 'jobs', 'legal', 'privacy', 'abuse', 'spam',
  'postmaster', 'hostmaster', 'newsletter', 'editor', 'news',
  'general', 'enquiries', 'enquiry',
]);

// These prefixes are generic but still usable (kept separately)
const GENERIC_PREFIXES = new Set(['info', 'contact', 'office', 'hello', 'inquiry']);

const SKIP_DOMAINS = new Set([
  'yelp.com', 'yellowpages.com', 'bbb.org', 'facebook.com', 'instagram.com',
  'linkedin.com', 'twitter.com', 'x.com', 'google.com', 'bing.com',
  'healthgrades.com', 'zocdoc.com', 'webmd.com', 'vitals.com', 'npiprofile.com',
  'angi.com', 'thumbtack.com', 'nextdoor.com', 'tripadvisor.com',
  'superpages.com', 'manta.com', 'chamberofcommerce.com', 'expertise.com',
  'realtor.com', 'zillow.com', 'redfin.com', 'trulia.com', 'cars.com',
  'autotrader.com', 'carmax.com', 'avvo.com', 'findlaw.com', 'justia.com',
  'indeed.com', 'glassdoor.com', 'amazon.com', 'walmart.com', 'irs.gov',
  'medicare.gov', 'hhs.gov', 'usa.gov', 'wix.com', 'squarespace.com',
  'shopify.com', 'godaddy.com', 'wordpress.com', 'progressive.com',
  'statefarm.com', 'allstate.com', 'nationwide.com', 'geico.com',
  'banfield.com', 'vca.com', 'petsmart.com', 'petco.com',
  'aspen dental', 'aspendental.com', 'heartlanddental.com',
  'kw.com', 'kellerwilliams.com', 'remax.com', 'coldwellbanker.com',
  'century21.com', 'compass.com',
]);

const FRANCHISE_KEYWORDS = [
  'state farm', 'allstate', 'geico', 'primerica', 'farmers insurance',
  'nationwide', 'liberty mutual', 'edward jones', 'ameriprise', 'raymond james',
  'northwestern mutual', 'keller williams', 're/max', 'remax', 'coldwell banker',
  'century 21', 'exp realty', 'aspen dental', 'heartland dental', 'banfield',
];

export class FreeScraperNode extends BaseNode {
  static nodeType = 'free_scraper';

  constructor(config, region = 'us-east-1', parentId = null) {
    super(config, region, parentId);

    this.targetIndustries = config.target_industries ?? [];
    this.targetLocations  = config.target_locations  ?? [];
    this.sources          = config.sources ?? ['yelp', 'bbb', 'google_organic', 'google_maps'];
    this.dailyCapPerCity  = config.daily_cap_per_city ?? 300;
    this.region_label     = config.region_label ?? 'all';

    this._scrapedDomains = new Set();
    this._scrapedToday   = new Map();
    this._browser        = null;
    this._uaIndex        = Math.floor(Math.random() * USER_AGENTS.length);
    this._locationIndex  = 0;

    // Pre-warm domain/email cache so we skip known businesses without touching the browser
    this._knownDomains   = new Set(); // populated from DynamoDB before first cycle
    this._cacheWarmed    = false;
    this._cycleCount     = 0;
    this._cacheRefreshEvery = config.cache_refresh_every ?? 10; // refresh from DB every N cycles
  }

  // ── Main cycle ─────────────────────────────────────────────────────────────

  async runCycle() {
    const tasks = await this.receiveTasks(10);
    for (const t of tasks) {
      if (t.command === 'shutdown' && (!t.target_node_id || t.target_node_id === this.nodeId)) {
        await this.stop('commander_shutdown');
      } else if (t.command === 'update_config') {
        this.config[t.key] = t.value;
      }
      await this.ackTask(t._receipt_handle);
    }

    this._cycleCount++;
    if (!this._cacheWarmed || this._cycleCount % this._cacheRefreshEvery === 0) {
      await this._warmDomainCache();
    }

    await this._scrapeAndStore();
  }

  // Loads all known domains + emails from DynamoDB so we never re-scrape an existing business.
  async _warmDomainCache() {
    try {
      const existing = await this.memory.getLeads(null, 10000);
      this._knownDomains.clear();
      for (const lead of existing) {
        if (lead.website) this._knownDomains.add(lead.website.toLowerCase().replace(/^www\./, ''));
        if (lead.email)   this._knownDomains.add(lead.email.toLowerCase());
      }
      this._cacheWarmed = true;
      log.info({ event: 'domain_cache_warmed', known: this._knownDomains.size, region: this.region_label });
    } catch (err) {
      log.warn({ event: 'domain_cache_warn', error: err.message });
    }
  }

  async _scrapeAndStore() {
    // Rotate to next city each cycle so we cover all locations over time
    const loc = this.targetLocations[this._locationIndex % this.targetLocations.length];
    this._locationIndex++;

    if (!loc) return;
    log.info({ event: 'cycle_start', region: this.region_label, location: loc });

    const freshLeads = [];

    for (const source of this.sources) {
      try {
        let leads = [];
        switch (source) {
          case 'yelp':           leads = await this._yelpScrape(loc);           break;
          case 'bbb':            leads = await this._bbbScrape(loc);            break;
          case 'google_organic': leads = await this._googleOrganicScrape(loc);  break;
          case 'google_maps':    leads = await this._googleMapsScrape(loc);     break;
        }
        freshLeads.push(...leads);
        this.increment('leads_found', leads.length);
        log.info({ event: 'source_done', source, location: loc, found: leads.length });
      } catch (err) {
        log.error({ event: 'source_error', source, error: err.message });
        this.increment('errors');
      }
      await sleep(jitter(3000, 2000));
    }

    const deduped = this._deduplicateLocally(freshLeads);
    let stored = 0;
    for (const lead of deduped) {
      if (!this._passesQualityGate(lead)) continue;

      // Final cache check — catches leads that came in from another parallel worker this cycle
      const dom = (lead.website ?? '').toLowerCase().replace(/^www\./, '');
      const em  = (lead.email  ?? '').toLowerCase();
      if (dom && this._knownDomains.has(dom)) continue;
      if (em  && this._knownDomains.has(em))  continue;

      await this.memory.upsertLead({ ...lead, source_node: this.nodeId });

      // Keep local cache up-to-date so subsequent leads in this batch don't repeat
      if (dom) this._knownDomains.add(dom);
      if (em)  this._knownDomains.add(em);

      if (lead.email) this.increment('leads_with_email');
      if (lead.confidence === 'guessed') this.increment('leads_guessed');
      stored++;
    }

    log.info({ event: 'cycle_done', location: loc, found: freshLeads.length, stored });
  }

  // ── Quality gate ──────────────────────────────────────────────────────────

  _passesQualityGate(lead) {
    if (!lead.email && !lead.phone) return false;
    if (!lead.website && !lead.company) return false;
    if (lead.has_chatbot) return false;
    // Reject if company name contains franchise keywords
    const name = (lead.company ?? '').toLowerCase();
    if (FRANCHISE_KEYWORDS.some(k => name.includes(k))) return false;
    return true;
  }

  // ── Source: Yelp ───────────────────────────────────────────────────────────

  async _yelpScrape(loc) {
    const leads   = [];
    const browser = await this._ensureBrowser();
    const cityKey = `yelp:${loc.toLowerCase()}`;
    if ((this._scrapedToday.get(cityKey) ?? 0) >= this.dailyCapPerCity) return leads;

    for (const industry of this.targetIndustries) {
      const category = YELP_CATEGORIES[industry] ?? industry.toLowerCase().replace(/\s+/g, '');
      const url      = `https://www.yelp.com/search?find_desc=${encodeURIComponent(category)}&find_loc=${encodeURIComponent(loc)}&start=0`;

      const ctx  = await browser.createBrowserContext();
      const page = await ctx.newPage();
      try {
        await this._prepPage(page);
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 25_000 });
        await sleep(jitter(2000, 1500));

        if (await this._isBlocked(page)) {
          log.warn({ event: 'yelp_blocked', url });
          continue;
        }

        const listings = await page.evaluate(() => {
          const results = [];
          document.querySelectorAll('[data-testid="serp-ia-card"], li[class*="result"]').forEach(el => {
            const nameEl = el.querySelector('a[class*="business-name"], h3 a, [class*="businessName"] a');
            const name   = nameEl?.textContent?.trim() ?? null;
            const href   = nameEl?.href ?? null;
            const phone  = el.querySelector('[class*="phone"]')?.textContent?.trim() ?? null;
            let website  = null;
            const siteEl = el.querySelector('a[href*="biz_redir"], a[href*="redirect"]');
            if (siteEl?.href) {
              try {
                const u = new URL(siteEl.href);
                website = u.searchParams.get('url') ?? siteEl.href;
              } catch { website = siteEl.href; }
            }
            if (name) results.push({ name, yelpUrl: href, website, phone });
          });
          return results;
        });

        for (const listing of listings) {
          let website = listing.website;
          if (!website && listing.yelpUrl) {
            website = await this._getYelpWebsite(ctx, listing.yelpUrl);
          }
          if (!website) continue;

          let domain;
          try { domain = new URL(website).hostname.replace(/^www\./, ''); } catch { continue; }
          if (this._skipDomain(domain)) continue;

          const enriched = await this._enrichDomain(website, domain, industry, loc);
          if (enriched) {
            for (const lead of [enriched].flat()) {
              leads.push({ ...lead, company: lead.company ?? listing.name, phone: lead.phone ?? listing.phone });
            }
          }
          this._scrapedToday.set(cityKey, (this._scrapedToday.get(cityKey) ?? 0) + 1);
          await sleep(jitter(1500, 2000));
        }

        log.info({ event: 'yelp_done', url, found: listings.length });
      } catch (err) {
        log.warn({ event: 'yelp_error', url, error: err.message });
      } finally {
        await page.close().catch(() => {});
        await ctx.close().catch(() => {});
      }
      await sleep(jitter(2000, 2000));
    }
    return leads;
  }

  async _getYelpWebsite(ctx, yelpUrl) {
    const page = await ctx.newPage();
    try {
      await this._prepPage(page);
      await page.goto(yelpUrl, { waitUntil: 'domcontentloaded', timeout: 20_000 });
      await sleep(jitter(1200, 800));
      return await page.evaluate(() => {
        const el = document.querySelector('a[href*="biz_redir"]') ?? document.querySelector('p a[target="_blank"]');
        if (!el) return null;
        try {
          const u = new URL(el.href);
          return u.searchParams.get('url') ?? el.href;
        } catch { return el.href; }
      });
    } catch { return null; }
    finally { await page.close().catch(() => {}); }
  }

  // ── Source: BBB ────────────────────────────────────────────────────────────

  async _bbbScrape(loc) {
    const leads   = [];
    const browser = await this._ensureBrowser();
    const cityKey = `bbb:${loc.toLowerCase()}`;
    if ((this._scrapedToday.get(cityKey) ?? 0) >= this.dailyCapPerCity) return leads;

    for (const industry of this.targetIndustries) {
      const term = BBB_TERMS[industry] ?? industry;
      const url  = `https://www.bbb.org/search?find_text=${encodeURIComponent(term)}&find_loc=${encodeURIComponent(loc)}&page=1`;

      const ctx  = await browser.createBrowserContext();
      const page = await ctx.newPage();
      try {
        await this._prepPage(page);
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 25_000 });
        await sleep(jitter(2000, 1500));

        if (await this._isBlocked(page)) { continue; }

        const listings = await page.evaluate(() => {
          const results = [];
          document.querySelectorAll('[class*="search-result"], .result-block').forEach(el => {
            const name    = el.querySelector('h2, h3, [class*="business-name"], a[class*="name"]')?.textContent?.trim() ?? null;
            const phone   = el.querySelector('[class*="phone"]')?.textContent?.trim() ?? null;
            let website   = el.querySelector('a[class*="website"]')?.href ?? null;
            const bbbHref = el.querySelector('a[href*="/profile/"]')?.href ?? null;
            if (!website) {
              const ext = el.querySelector('a[target="_blank"][href*="www."]:not([href*="bbb.org"])');
              if (ext) website = ext.href;
            }
            if (name) results.push({ name, phone, website, bbbHref });
          });
          return results;
        });

        for (const listing of listings) {
          let website = listing.website;
          if (!website && listing.bbbHref) {
            website = await this._getBBBWebsite(ctx, listing.bbbHref);
          }
          if (!website) continue;

          let domain;
          try { domain = new URL(website).hostname.replace(/^www\./, ''); } catch { continue; }
          if (this._skipDomain(domain)) continue;

          const enriched = await this._enrichDomain(website, domain, industry, loc);
          if (enriched) {
            for (const lead of [enriched].flat()) {
              leads.push({ ...lead, company: lead.company ?? listing.name, phone: lead.phone ?? listing.phone });
            }
          }
          this._scrapedToday.set(cityKey, (this._scrapedToday.get(cityKey) ?? 0) + 1);
          await sleep(jitter(1500, 1500));
        }

        log.info({ event: 'bbb_done', url, found: listings.length });
      } catch (err) {
        log.warn({ event: 'bbb_error', url, error: err.message });
      } finally {
        await page.close().catch(() => {});
        await ctx.close().catch(() => {});
      }
      await sleep(jitter(2000, 2000));
    }
    return leads;
  }

  async _getBBBWebsite(ctx, bbbUrl) {
    const page = await ctx.newPage();
    try {
      await this._prepPage(page);
      await page.goto(bbbUrl, { waitUntil: 'domcontentloaded', timeout: 20_000 });
      await sleep(jitter(1000, 700));
      return await page.evaluate(() => {
        const el = document.querySelector('a[class*="website"]')
                ?? document.querySelector('a[target="_blank"][href*="www."]:not([href*="bbb.org"])');
        return el?.href ?? null;
      });
    } catch { return null; }
    finally { await page.close().catch(() => {}); }
  }

  // ── Source: Google Organic ─────────────────────────────────────────────────

  async _googleOrganicScrape(loc) {
    const leads   = [];
    const browser = await this._ensureBrowser();
    const cityKey = `google:${loc.toLowerCase()}`;
    if ((this._scrapedToday.get(cityKey) ?? 0) >= this.dailyCapPerCity) return leads;

    for (const industry of this.targetIndustries) {
      const queries = [
        `${industry} in ${loc}`,
        `best ${industry} ${loc.split(',')[0]}`,
        `local ${industry} near ${loc.split(',')[0]}`,
      ];

      for (const query of queries) {
        const ctx  = await browser.createBrowserContext();
        const page = await ctx.newPage();
        try {
          await this._prepPage(page);
          const searchUrl = `https://www.google.com/search?q=${encodeURIComponent(query)}&num=20&gl=us&hl=en`;
          await page.goto(searchUrl, { waitUntil: 'domcontentloaded', timeout: 25_000 });
          await sleep(jitter(2000, 1200));

          if (await this._isBlocked(page)) {
            log.warn({ event: 'google_blocked', query });
            await sleep(jitter(8000, 5000));
            continue;
          }

          const sites = await page.evaluate(() => {
            const results = [];
            document.querySelectorAll('#search a[href], #rso a[href], .yuRUbf a').forEach(anchor => {
              const href = anchor.href;
              if (!href || !href.startsWith('http')) return;
              try {
                const domain = new URL(href).hostname.replace(/^www\./, '');
                results.push({ url: href, domain });
              } catch {}
            });
            return results;
          });

          const seen = new Set();
          for (const site of sites) {
            if (seen.has(site.domain)) continue;
            seen.add(site.domain);
            if (this._skipDomain(site.domain)) continue;

            const enriched = await this._enrichDomain(site.url, site.domain, industry, loc);
            if (enriched) {
              for (const lead of [enriched].flat()) leads.push(lead);
            }
            this._scrapedToday.set(cityKey, (this._scrapedToday.get(cityKey) ?? 0) + 1);
            await sleep(jitter(2000, 1500));
          }

          log.info({ event: 'google_done', query, sites: sites.length });
        } catch (err) {
          log.warn({ event: 'google_error', query, error: err.message });
        } finally {
          await page.close().catch(() => {});
          await ctx.close().catch(() => {});
        }
        await sleep(jitter(3000, 2000));
      }
    }
    return leads;
  }

  // ── Source: Google Maps ────────────────────────────────────────────────────

  async _googleMapsScrape(loc) {
    const leads   = [];
    const browser = await this._ensureBrowser();
    const cityKey = `maps:${loc.toLowerCase()}`;
    if ((this._scrapedToday.get(cityKey) ?? 0) >= this.dailyCapPerCity) return leads;

    for (const industry of this.targetIndustries) {
      const query = `${industry} near ${loc}`;
      const url   = `https://www.google.com/maps/search/${encodeURIComponent(query)}`;

      const ctx  = await browser.createBrowserContext();
      const page = await ctx.newPage();
      try {
        await this._prepPage(page);
        await page.goto(url, { waitUntil: 'networkidle2', timeout: 30_000 });
        await sleep(jitter(3000, 2000));

        if (await this._isBlocked(page)) {
          log.warn({ event: 'maps_blocked', query });
          continue;
        }

        // Scroll sidebar to load more results
        for (let i = 0; i < 3; i++) {
          await page.evaluate(() => {
            const feed = document.querySelector('[role="feed"]')
                      ?? document.querySelector('div[aria-label*="Results"]');
            if (feed) feed.scrollTop += 600;
          });
          await sleep(jitter(1000, 500));
        }

        const listings = await page.evaluate(() => {
          const results = [];
          const cards = document.querySelectorAll('[role="feed"] a[href*="/maps/place/"], div.Nv2PK a');
          const seen  = new Set();
          for (const card of cards) {
            const href = card.href;
            if (!href || seen.has(href)) continue;
            seen.add(href);
            const parent = card.closest('[role="article"]') ?? card.parentElement;
            const name   = parent?.querySelector('div.fontHeadlineSmall, .qBF1Pd')?.textContent?.trim()
                        ?? card.textContent?.trim() ?? null;
            if (name && href) results.push({ name, mapsUrl: href });
          }
          return results.slice(0, 25);
        });

        log.info({ event: 'maps_found', query, count: listings.length });

        for (const listing of listings) {
          const website = await this._getMapsWebsite(ctx, listing.mapsUrl);
          if (!website) continue;

          let domain;
          try { domain = new URL(website).hostname.replace(/^www\./, ''); } catch { continue; }
          if (this._skipDomain(domain)) continue;

          const enriched = await this._enrichDomain(website, domain, industry, loc);
          if (enriched) {
            for (const lead of [enriched].flat()) {
              leads.push({ ...lead, company: lead.company ?? listing.name });
            }
          }
          this._scrapedToday.set(cityKey, (this._scrapedToday.get(cityKey) ?? 0) + 1);
          await sleep(jitter(2000, 1500));
        }
      } catch (err) {
        log.warn({ event: 'maps_error', query, error: err.message });
      } finally {
        await page.close().catch(() => {});
        await ctx.close().catch(() => {});
      }
      await sleep(jitter(3000, 2000));
    }
    return leads;
  }

  async _getMapsWebsite(ctx, mapsUrl) {
    const page = await ctx.newPage();
    try {
      await this._prepPage(page);
      await page.goto(mapsUrl, { waitUntil: 'domcontentloaded', timeout: 20_000 });
      await sleep(jitter(2000, 1000));
      return await page.evaluate(() => {
        // Website button in Maps place panel
        const candidates = [
          ...document.querySelectorAll('a[data-item-id="authority"]'),
          ...document.querySelectorAll('a[aria-label*="website"]'),
          ...document.querySelectorAll('a[jsaction*="pane.rating.moreReviews"]'),
        ];
        for (const el of candidates) {
          try {
            const u = new URL(el.href);
            if (!u.hostname.includes('google')) return el.href;
          } catch {}
        }
        return null;
      });
    } catch { return null; }
    finally { await page.close().catch(() => {}); }
  }

  // ── Email enrichment waterfall (3 layers) ─────────────────────────────────

  async _enrichDomain(url, domain, industry, location) {
    const cleanDomain = domain.toLowerCase().replace(/^www\./, '');
    if (this._scrapedDomains.has(cleanDomain)) return null;
    if (this._knownDomains.has(cleanDomain)) {
      log.debug({ event: 'skip_known', domain: cleanDomain });
      return null;
    }
    this._scrapedDomains.add(cleanDomain);

    const base = {
      email:       null,
      phone:       null,
      name:        null,
      title:       null,
      company:     null,
      website:     domain,
      industry,
      location,
      has_chatbot: false,
      site_tagline: null,
      confidence:  null,
      verified:    null,
      linkedin_url: null,
      source:      'free_scraper',
    };

    // ── Layer 1: Deep website scrape ────────────────────────────────────────
    const site = await this._deepScrapeWebsite(url, domain, industry);
    base.phone     = site.phone;
    base.name      = site.ownerName;
    base.company   = site.siteName;
    base.has_chatbot = site.hasChatbot;
    base.site_tagline = site.tagline;

    if (site.hasChatbot) {
      this.increment('chatbots_found');
      return null;
    }

    if (site.emails.length > 0) {
      // Return up to 2 real scraped emails as separate leads
      return site.emails.slice(0, 2).map(email => ({ ...base, email, confidence: 'scraped' }));
    }

    // ── Layer 2: RDAP/WHOIS registrant lookup ───────────────────────────────
    const whoisEmail = await this._whoisEmail(domain);
    if (whoisEmail) {
      return { ...base, email: whoisEmail, confidence: 'whois' };
    }

    // ── Layer 3: Pattern guess + MX validation ──────────────────────────────
    const guessed = await this._guessEmail(domain, site.ownerName);
    if (guessed) {
      return { ...base, email: guessed, confidence: 'guessed' };
    }

    // Keep phone-only leads — still reachable
    if (base.phone) return { ...base, confidence: 'phone_only' };

    return null;
  }

  // ── Deep website scrape ───────────────────────────────────────────────────

  async _deepScrapeWebsite(url, domain, industry) {
    const result = {
      emails: [], phone: null, ownerName: null,
      hasChatbot: false, chatbotName: null, tagline: null, siteName: null,
    };

    const browser = await this._ensureBrowser();
    const ctx     = await browser.createBrowserContext();
    const page    = await ctx.newPage();

    try {
      await page.setRequestInterception(true);
      page.on('request', req => {
        const t = req.resourceType();
        if (['image', 'media', 'font', 'stylesheet'].includes(t)) req.abort();
        else req.continue();
      });

      await this._prepPage(page);
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 18_000 });
      await sleep(jitter(1200, 800));

      const title = await page.title().catch(() => '');
      if (this._isChallengePage(title)) return result;

      const homeData = await this._extractPageData(page);
      this._mergeInto(result, homeData);

      // Generic contact pages
      const basePath = `https://${domain}`;
      const generic  = ['/contact', '/contact-us', '/about', '/about-us', '/team', '/our-team', '/staff'];
      const specific = INDUSTRY_CONTACT_PATHS[industry] ?? [];
      const allPaths = [...new Set([...generic, ...specific])];

      for (const path of allPaths) {
        if (result.emails.length >= 2 && result.phone && result.ownerName) break;
        try {
          await page.goto(`${basePath}${path}`, { waitUntil: 'domcontentloaded', timeout: 10_000 });
          await sleep(jitter(600, 500));
          this._mergeInto(result, await this._extractPageData(page));
        } catch {}
      }

      // Smart internal link scan if still no email
      if (result.emails.length === 0) {
        const internalLinks = await page.evaluate((dom) => {
          return [...document.querySelectorAll('a[href]')]
            .map(a => a.href)
            .filter(h => h.startsWith('http') && h.includes(dom));
        }, domain).catch(() => []);

        const scored = [...new Set(internalLinks)]
          .map(href => {
            try { return { href, score: 0 }; } catch { return null; }
          })
          .filter(Boolean)
          .map(l => ({
            href:  l.href,
            score: this._scoreContactPath(new URL(l.href).pathname.toLowerCase()),
          }))
          .filter(l => l.score > 0)
          .sort((a, b) => b.score - a.score)
          .slice(0, 4);

        for (const link of scored) {
          if (result.emails.length > 0) break;
          try {
            await page.goto(link.href, { waitUntil: 'domcontentloaded', timeout: 10_000 });
            await sleep(jitter(600, 400));
            this._mergeInto(result, await this._extractPageData(page));
          } catch {}
        }
      }
    } catch (err) {
      log.debug({ event: 'site_error', domain, error: err.message });
    } finally {
      await page.close().catch(() => {});
      await ctx.close().catch(() => {});
    }

    // Strip junk prefixes from emails
    result.emails = [...new Set(result.emails)].filter(email => {
      const prefix = email.split('@')[0].toLowerCase().replace(/[^a-z]/g, '');
      return !JUNK_PREFIXES.has(prefix);
    });

    return result;
  }

  async _extractPageData(page) {
    return page.evaluate((sigs) => {
      const html     = document.documentElement.outerHTML.toLowerCase();
      const bodyText = document.body?.innerText ?? '';

      // Emails — from text + mailto hrefs
      const re = /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/g;
      const raw = [
        ...(bodyText.match(re) || []),
        ...(html.match(/mailto:([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})/gi) || [])
          .map(m => m.replace(/^mailto:/i, '')),
      ];
      const emails = [...new Set(raw)].filter(e => {
        const l = e.toLowerCase();
        return !l.includes('example.com') && !l.includes('sentry') && !l.includes('wixpress')
            && !l.includes('.png') && !l.includes('.jpg') && !l.includes('.css')
            && !l.endsWith('.js') && l.length < 80;
      });

      // Phone
      const phones = bodyText.match(/(?:\+1[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}/g) ?? [];
      const phone  = phones[0]?.replace(/[^\d+]/g, '') ?? null;

      // Chatbot
      let hasChatbot = false, chatbotName = null;
      for (const s of [...document.querySelectorAll('script[src]')]) {
        const src = s.src.toLowerCase();
        for (const sig of sigs) {
          if (src.includes(sig)) { hasChatbot = true; chatbotName = sig; break; }
        }
        if (hasChatbot) break;
      }
      if (!hasChatbot) {
        for (const sig of sigs) {
          if (html.includes(sig)) { hasChatbot = true; chatbotName = sig; break; }
        }
      }
      if (!hasChatbot) {
        const chatSels = [
          '#intercom-container', '#drift-widget', '.tidio-chat',
          '#livechat-compact-container', '#hubspot-messages-iframe-container',
          '#crisp-chatbox', '.tawk-min-container', '#olark-wrapper',
          '[class*="chat-widget"]', '[id*="chat-widget"]',
          'iframe[src*="livechat"]', 'iframe[src*="tawk"]',
        ];
        for (const sel of chatSels) {
          if (document.querySelector(sel)) { hasChatbot = true; chatbotName = sel; break; }
        }
      }

      // Owner name
      let ownerName = null;
      const ownerPats = [
        /(?:owner|founder|ceo|president|principal|dr\.?|dds|dmd|pa-c|np)[:\s,]+([A-Z][a-z]+ [A-Z][a-z]+)/i,
        /([A-Z][a-z]+ [A-Z][a-z]+)[,\s]+(?:owner|founder|ceo|president|dds|dmd|attorney)/i,
        /(?:meet\s+(?:dr\.?\s+)?|about\s+(?:dr\.?\s+)?)([A-Z][a-z]+ [A-Z][a-z]+)/i,
      ];
      for (const pat of ownerPats) {
        const m = bodyText.match(pat);
        if (m) { ownerName = m[1].trim(); break; }
      }

      const metaDesc = document.querySelector('meta[name="description"]')?.getAttribute('content')
                    ?? document.querySelector('meta[property="og:description"]')?.getAttribute('content')
                    ?? null;
      const siteName = document.querySelector('meta[property="og:site_name"]')?.getAttribute('content')
                    ?? document.title ?? null;

      return { emails, phone, hasChatbot, chatbotName, ownerName,
               tagline: (metaDesc ?? '').slice(0, 200).trim() || null, siteName };
    }, CHATBOT_SIGNATURES);
  }

  _mergeInto(target, src) {
    target.emails      = [...target.emails, ...(src.emails ?? [])];
    target.phone       = target.phone      || src.phone;
    target.ownerName   = target.ownerName  || src.ownerName;
    target.hasChatbot  = target.hasChatbot || src.hasChatbot;
    target.chatbotName = target.chatbotName|| src.chatbotName;
    target.tagline     = target.tagline    || src.tagline;
    target.siteName    = target.siteName   || src.siteName;
  }

  // ── Layer 2: RDAP/WHOIS ───────────────────────────────────────────────────

  async _whoisEmail(domain) {
    try {
      const resp = await axios.get(`https://rdap.org/domain/${domain}`, {
        timeout: 6000,
        headers: { Accept: 'application/json' },
      });
      for (const entity of resp.data?.entities ?? []) {
        const roles = entity?.roles ?? [];
        if (!roles.includes('registrant') && !roles.includes('administrative')) continue;
        for (const field of entity?.vcardArray?.[1] ?? []) {
          if (field[0] === 'email' && field[3]) {
            const e = field[3];
            if (e.includes('@') && !e.includes('redacted') && !e.includes('privacy')) return e;
          }
        }
      }
    } catch {}
    return null;
  }

  // ── Layer 3: Email pattern + MX check ────────────────────────────────────

  async _guessEmail(domain, ownerName) {
    const hasMx = await this._checkMx(domain);
    if (!hasMx) return null;

    const candidates = [];
    if (ownerName) {
      const parts = ownerName.trim().split(/\s+/);
      const first = parts[0]?.toLowerCase().replace(/[^a-z]/g, '') ?? '';
      const last  = parts[parts.length - 1]?.toLowerCase().replace(/[^a-z]/g, '') ?? '';
      const fi    = first[0] ?? '';
      if (first && last) {
        candidates.push(
          `${first}@${domain}`,
          `${first}.${last}@${domain}`,
          `${fi}${last}@${domain}`,
          `${fi}.${last}@${domain}`,
          `${first}${last}@${domain}`,
        );
      }
    }
    // Generic fallbacks kept separate — lower priority
    candidates.push(`info@${domain}`, `contact@${domain}`, `hello@${domain}`);

    return candidates[0] ?? null;
  }

  async _checkMx(domain) {
    try {
      const records = await dns.resolveMx(domain);
      return records?.length > 0;
    } catch { return false; }
  }

  // ── Stealth + browser helpers ─────────────────────────────────────────────

  async _ensureBrowser() {
    if (!this._browser || !this._browser.isConnected()) {
      this._browser = await puppeteer.launch({
        headless: 'new',
        args: [
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--disable-dev-shm-usage',
          '--disable-gpu',
          '--disable-blink-features=AutomationControlled',
          '--window-size=1366,768',
          '--lang=en-US,en',
          '--no-first-run',
          '--disable-infobars',
          '--ignore-certificate-errors',
          '--disable-notifications',
        ],
        ignoreDefaultArgs: ['--enable-automation'],
      });
    }
    return this._browser;
  }

  async _prepPage(page) {
    const ua = USER_AGENTS[this._uaIndex++ % USER_AGENTS.length];
    await page.setUserAgent(ua);
    const w = [1280, 1366, 1440, 1536, 1920][Math.floor(Math.random() * 5)];
    const h = [720, 768, 800, 864, 1080][Math.floor(Math.random() * 5)];
    await page.setViewport({ width: w, height: h });
    await page.setExtraHTTPHeaders({
      'Accept-Language':        'en-US,en;q=0.9',
      'Accept':                 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Encoding':        'gzip, deflate, br',
      'DNT':                    '1',
      'Upgrade-Insecure-Requests': '1',
    });
    await page.evaluateOnNewDocument(() => {
      Object.defineProperty(navigator, 'webdriver',  { get: () => false });
      Object.defineProperty(navigator, 'languages',  { get: () => ['en-US', 'en'] });
      Object.defineProperty(navigator, 'plugins',    { get: () => [1, 2, 3] });
      window.chrome = { runtime: {} };
    });
  }

  async _isBlocked(page) {
    const t = await page.title().catch(() => '');
    return this._isChallengePage(t);
  }

  _isChallengePage(title) {
    const t = (title ?? '').toLowerCase();
    return ['just a moment', 'attention required', 'security check', 'ddos protection',
            'please wait', 'checking your browser', 'verify you are human'].some(p => t.includes(p));
  }

  _skipDomain(domain) {
    if (!domain) return true;
    if (this._scrapedDomains.has(domain)) return true;
    if (domain.endsWith('.gov') || domain.endsWith('.mil') || domain.endsWith('.edu')) return true;
    for (const skip of SKIP_DOMAINS) {
      if (domain.includes(skip)) return true;
    }
    return false;
  }

  _scoreContactPath(path) {
    const high   = ['contact', 'team', 'staff', 'attorney', 'doctor', 'provider', 'agent', 'advisor', 'meet', 'bio', 'profile'];
    const medium = ['about', 'people', 'reach', 'location', 'office', 'directory', 'roster'];
    if (high.some(k => path.includes(k)))   return 3;
    if (medium.some(k => path.includes(k))) return 2;
    return 0;
  }

  // ── Deduplication ─────────────────────────────────────────────────────────

  _deduplicateLocally(leads) {
    const flat = leads.flat().filter(Boolean);
    const seen = new Set();
    return flat.filter(lead => {
      const key = (lead.email ?? lead.website ?? '').toLowerCase();
      if (!key || seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }

  // ── BaseNode ──────────────────────────────────────────────────────────────

  async stop(reason) {
    if (this._browser) {
      try { await this._browser.close(); } catch {}
      this._browser = null;
    }
    return super.stop(reason);
  }

  collectMetrics() {
    const found   = this.getCounter('leads_found');
    const withEmail = this.getCounter('leads_with_email');
    return {
      leads_found:      found,
      leads_with_email: withEmail,
      leads_guessed:    this.getCounter('leads_guessed'),
      chatbots_found:   this.getCounter('chatbots_found'),
      errors:           this.getCounter('errors'),
      email_rate:       found > 0 ? withEmail / found : 0,
    };
  }

  getImprovementContext() {
    return {
      node_type:         this.nodeType,
      region_label:      this.region_label,
      sources:           this.sources,
      target_industries: this.targetIndustries,
      locations_count:   this.targetLocations.length,
      metrics:           this.collectMetrics(),
    };
  }
}
