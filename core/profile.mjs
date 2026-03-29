/**
 * core/profile.mjs — Business Profile Loader
 *
 * Reads profile.json and translates it into concrete configs for each node type.
 * Every node and the Commander reads from this so the whole swarm is driven
 * by a single, human-readable business profile.
 */

import { readFileSync, existsSync } from 'fs';
import { resolve }                  from 'path';
import { log }                      from './logger.mjs';

const PROFILE_PATH = resolve(process.cwd(), 'profile.json');

// ------------------------------------------------------------------ //
//  Load                                                               //
// ------------------------------------------------------------------ //

export function loadProfile() {
  if (!existsSync(PROFILE_PATH)) {
    log.warn({ event: 'profile_not_found', path: PROFILE_PATH });
    return null;
  }
  try {
    const raw     = readFileSync(PROFILE_PATH, 'utf-8');
    const profile = JSON.parse(raw);
    delete profile._instructions; // strip the help text key if present
    log.info({ event: 'profile_loaded', business: profile.business?.name });
    return profile;
  } catch (err) {
    log.error({ event: 'profile_parse_error', error: err.message });
    return null;
  }
}

// ------------------------------------------------------------------ //
//  Node config generators                                              //
// ------------------------------------------------------------------ //

/**
 * Config injected into every email_node instance.
 */
export function emailNodeConfig(profile) {
  if (!profile) return {};
  const { sender, offer, icp, email_sequences, business } = profile;
  return {
    // Sender identity
    from_name:        sender?.from_name        ?? business?.name,
    signature:        sender?.signature        ?? '',
    persona:          sender?.persona          ?? '',

    // Offer
    offer_name:       offer?.primary?.name     ?? '',
    offer_price:      offer?.primary?.price    ?? '',
    offer_trial:      offer?.primary?.trial    ?? '',
    offer_cta_url:    offer?.primary?.cta_url  ?? business?.website ?? '',
    pain_solved:      offer?.primary?.pain_solved ?? '',

    // Sequence settings
    sequence_length:  email_sequences?.sequence_length   ?? 3,
    follow_up_days:   email_sequences?.follow_up_days    ?? [0, 3, 7],
    send_window_start: email_sequences?.send_window_start ?? '08:00',
    send_window_end:  email_sequences?.send_window_end   ?? '17:00',
    send_days:        email_sequences?.send_days         ?? ['Monday','Tuesday','Wednesday','Thursday'],
    tone:             email_sequences?.tone              ?? 'conversational and direct',

    // Templates as generation context (GPT uses these as seed instructions)
    sequence_templates: email_sequences?.templates       ?? {},

    // ICP context so GPT can personalise per-lead
    icp_description:  icp?.description   ?? '',
    icp_pain_points:  icp?.pain_points   ?? [],
    icp_industries:   icp?.industries    ?? [],

    // Business context
    business_name:    business?.name     ?? '',
    business_website: business?.website  ?? '',
    differentiators:  business?.differentiators ?? [],
    value_anchor:     offer?.value_anchor ?? '',
  };
}

/**
 * Config injected into every scraper_node instance.
 */
export function scraperNodeConfig(profile) {
  if (!profile) return {};
  const { icp, business } = profile;

  // Map profile industries to Hunter.io industry names where possible
  const hunterIndustryMap = {
    'dental offices':       'Dentists',
    'dental':               'Dentists',
    'dentists':             'Dentists',
    'real estate':          'Real Estate',
    'real estate agencies': 'Real Estate',
    'law firms':            'Law Practice',
    'legal':                'Law Practice',
    'accounting':           'Accounting',
    'restaurants':          'Restaurants',
    'fitness':              'Health, Wellness and Fitness',
    'gyms':                 'Health, Wellness and Fitness',
    'medical':              'Medical Practices',
    'medical clinics':      'Medical Practices',
    'veterinary':           'Veterinary Services',
    'veterinary clinics':   'Veterinary Services',
    'plumbing':             'Plumbing, Heating, and Air-Conditioning Contractors',
    'hvac':                 'Plumbing, Heating, and Air-Conditioning Contractors',
    'auto repair':          'Automotive',
    'auto repair shops':    'Automotive',
    'insurance':            'Insurance',
    'insurance agencies':   'Insurance',
    'chiropractic':         'Alternative Medicine',
    'chiropractic offices': 'Alternative Medicine',
    'financial advisors':   'Financial Services',
    'financial':            'Financial Services',
  };

  // US state abbreviation lookup
  const stateAbbr = {
    'AL':'AL','AK':'AK','AZ':'AZ','AR':'AR','CA':'CA','CO':'CO','CT':'CT','DE':'DE',
    'FL':'FL','GA':'GA','HI':'HI','ID':'ID','IL':'IL','IN':'IN','IA':'IA','KS':'KS',
    'KY':'KY','LA':'LA','ME':'ME','MD':'MD','MA':'MA','MI':'MI','MN':'MN','MS':'MS',
    'MO':'MO','MT':'MT','NE':'NE','NV':'NV','NH':'NH','NJ':'NJ','NM':'NM','NY':'NY',
    'NC':'NC','ND':'ND','OH':'OH','OK':'OK','OR':'OR','PA':'PA','RI':'RI','SC':'SC',
    'SD':'SD','TN':'TN','TX':'TX','UT':'UT','VT':'VT','VA':'VA','WA':'WA','WV':'WV',
    'WI':'WI','WY':'WY',
  };

  const profileIndustries = icp?.industries ?? [];
  const hunterIndustries  = profileIndustries
    .map(i => hunterIndustryMap[i.toLowerCase()] ?? null)
    .filter(Boolean);

  // Parse locations into Hunter format: [{ country, state, city }]
  const hunterLocations = (icp?.locations ?? [])
    .filter(l => l !== 'United States')
    .map(loc => {
      const parts = loc.split(',').map(s => s.trim());
      if (parts.length >= 2) {
        const st = stateAbbr[parts[1].toUpperCase()] ?? parts[1];
        return { country: 'US', state: st, city: parts[0] };
      }
      return { country: 'US' };
    });

  return {
    target_job_titles:       icp?.job_titles          ?? [],
    target_industries:       icp?.industries          ?? [],
    company_size_min:        icp?.company_size_min    ?? 1,
    company_size_max:        icp?.company_size_max    ?? 50,
    target_locations:        icp?.locations           ?? ['United States'],
    exclude_keywords:        icp?.exclude_keywords    ?? [],
    has_website:             icp?.has_website         ?? true,
    icp_description:         icp?.description         ?? '',
    source_business:         business?.name           ?? '',
    daily_cap_per_city:      200,
    leads_per_cycle:         500,
    hunter_industries:       hunterIndustries,
    hunter_locations:        hunterLocations,
    hunter_headcount:        ['1-10', '11-50'],
    // yellowpages = local directory scrape (free, best for small city local businesses)
    // web_scrape  = Bing+Puppeteer (free, supplements YP with organic results)
    // hunter_discover = Hunter.io company discovery (requires HUNTER_API_KEY)
    sources:                 ['yellowpages', 'web_scrape'],
  };
}

/**
 * Config injected into seo_node instances.
 */
export function seoNodeConfig(profile) {
  if (!profile) return {};
  const { seo, business, offer } = profile;
  return {
    primary_keywords:     seo?.primary_keywords      ?? [],
    content_angles:       seo?.content_angles        ?? [],
    blog_topics_per_run:  seo?.target_blog_topics    ?? 3,
    publish_frequency:    seo?.publish_frequency     ?? '2x per week',
    business_name:        business?.name             ?? '',
    business_website:     business?.website          ?? '',
    differentiators:      business?.differentiators  ?? [],
    offer_summary:        offer?.primary?.name
      ? `${offer.primary.name} — ${offer.primary.price}`
      : '',
  };
}

/**
 * Config injected into analytics_node instances.
 */
export function analyticsNodeConfig(profile) {
  if (!profile) return {};
  const { business, campaign_goals } = profile;
  return {
    business_name:   business?.name    ?? '',
    business_website: business?.website ?? '',
    target_metrics:  (campaign_goals ?? []).map(g => ({
      campaign_name:  g.name,
      channel:        g.channel,
      success_metric: g.success_metric,
      target_rate:    g.target_rate,
    })),
  };
}

/**
 * Bootstrap goals array passed to Commander.bootstrapInitialCampaigns().
 */
export function bootstrapGoals(profile) {
  if (!profile?.campaign_goals?.length) return [];
  const { business, offer, sender } = profile;
  return profile.campaign_goals.map(g => ({
    name:             g.name,
    primary_channel:  g.channel ?? 'email_node',
    budget:           g.daily_budget_usd ?? 5,
    audience: {
      industries:    g.primary_industry ? [g.primary_industry] : (profile.icp?.industries ?? []),
      job_titles:    profile.icp?.job_titles ?? [],
      locations:     profile.icp?.locations  ?? [],
      size_max:      profile.icp?.company_size_max ?? 50,
    },
    daily_email_cap:  g.daily_email_cap ?? 50,
    success_metric:   g.success_metric  ?? 'reply_rate',
    target_rate:      g.target_rate     ?? 0.03,
    // Pass key context so the LLM can write better campaign briefs
    business_name:    business?.name    ?? '',
    offer_summary:    offer?.primary ? `${offer.primary.name} — ${offer.primary.price} — ${offer.primary.trial}` : '',
    sender_persona:   sender?.persona   ?? '',
  }));
}

/**
 * Merge profile-derived config with any manual overrides.
 * Used by run.mjs so CLI overrides always win.
 */
export function mergeProfileConfig(nodeType, profile, overrides = {}) {
  const generators = {
    email_node:     emailNodeConfig,
    scraper_node:   scraperNodeConfig,
    seo_node:       seoNodeConfig,
    analytics_node: analyticsNodeConfig,
  };
  const base = generators[nodeType]?.(profile) ?? {};
  return { ...base, ...overrides };
}
