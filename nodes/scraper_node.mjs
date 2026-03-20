/**
 * ScraperNode — Feeds fresh leads into all other swarm nodes.
 *
 * Capabilities:
 *   - Multi-source scraping: LinkedIn, Apollo.io, Hunter.io, Reddit, directories
 *   - Enrichment: finds email, phone, LinkedIn URL from company domain
 *   - Deduplication before writing to SharedMemory
 *   - Pushes leads into the unified lead database
 *   - Configurable targeting filters (industry, title, company size)
 *   - Unlimited lead pipeline with rotating sources
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
    this.sources         = config.sources ?? ['apollo', 'hunter', 'reddit'];
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
          case 'apollo':   leads = await this._scrapeApollo();   break;
          case 'hunter':   leads = await this._scrapeHunter();   break;
          case 'reddit':   leads = await this._scrapeReddit();   break;
          case 'linkedin': leads = await this._scrapeLinkedIn(); break;
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

  async _scrapeApollo() {
    if (!this.apolloApiKey) return [];
    const resp = await axios.post(
      'https://api.apollo.io/v1/mixed_people/search',
      {
        api_key:          this.apolloApiKey,
        person_titles:    this.targetTitles,
        organization_num_employees_ranges: [
          `${this.minCompanySize},${this.maxCompanySize}`,
        ],
        q_organization_industries: this.targetIndustries,
        per_page: Math.min(this.leadsPerCycle, 100),
      },
      { timeout: 20_000 },
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
    }));
  }

  async _scrapeHunter() {
    if (!this.hunterApiKey || !this.targetIndustries.length) return [];
    // Hunter domain search for a target company domain
    const domain = this.config.target_domain;
    if (!domain) return [];

    const resp = await axios.get('https://api.hunter.io/v2/domain-search', {
      params: { domain, api_key: this.hunterApiKey, limit: 20 },
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
