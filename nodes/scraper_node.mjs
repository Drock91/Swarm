/**
 * ScraperNode — Feeds fresh leads into all other swarm nodes.
 *
 * Capabilities:
 *   - Hunter Discover: finds companies by location, industry, headcount (FREE)
 *   - Hunter Domain Search: extracts verified emails from company domains
 *   - Apollo.io (paid plans): bulk people search by title/industry/location
 *   - Reddit: finds pain-point signals from relevant subreddits
 *   - Deduplication before writing to SharedMemory
 *   - Pushes leads into the unified lead database
 *   - Configurable targeting filters (industry, title, company size, location)
 */

import axios from 'axios';
import * as cheerio from 'cheerio';
import OpenAI from 'openai';
import { BaseNode } from '../core/base_node.mjs';
import { log } from '../core/logger.mjs';

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
    this.sources         = config.sources ?? ['hunter_discover', 'apollo', 'reddit'];

    // Hunter Discover params (mapped from profile ICP)
    this.hunterIndustries = config.hunter_industries ?? [];
    this.hunterHeadcount  = config.hunter_headcount  ?? ['1-10', '11-50'];
    this.hunterLocations  = config.hunter_locations   ?? [];      // [{ country, state, city }]

    // Track which domains we've already searched (to avoid burning search credits)
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
          case 'hunter_discover': leads = await this._hunterDiscover(); break;
          case 'apollo':          leads = await this._scrapeApollo();   break;
          case 'hunter':          leads = await this._scrapeHunter();   break;
          case 'reddit':          leads = await this._scrapeReddit();   break;
          case 'linkedin':        leads = await this._scrapeLinkedIn(); break;
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

  /**
   * Hunter Discover → Domain Search pipeline (primary, FREE discover calls).
   * 1. Discover: finds company domains by location + industry + headcount
   * 2. Domain Search: extracts personal emails from those domains (1 credit each)
   */
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
