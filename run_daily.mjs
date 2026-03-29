/**
 * run_daily.mjs — Production daily outreach runner
 *
 * Scrapes fresh cities from your profile list, then sends up to 290 emails
 * from the full accumulated lead database (including all prior days).
 *
 * Run once per day:  node run_daily.mjs
 * Or on a schedule: Windows Task Scheduler / cron
 *
 * Progress is saved in scrape_progress.json so cities rotate automatically.
 * After all cities are exhausted it resets and starts over.
 */

import 'dotenv/config';
import { readFileSync, writeFileSync, existsSync } from 'fs';
import { ScraperNode }                             from './nodes/scraper_node.mjs';
import { EmailNode }                               from './nodes/email_node.mjs';
import { loadProfile, scraperNodeConfig,
         emailNodeConfig }                         from './core/profile.mjs';

// ─── Config ───────────────────────────────────────────────────────────────────

const DAILY_SEND_TARGET = 290;   // Just under Brevo's 300/day limit
const CITIES_PER_RUN    = 4;     // Fresh cities to scrape each run
const LEADS_PER_CITY    = 300;   // YP finds ~300 per city across 10 industries
const PROGRESS_FILE     = './scrape_progress.json';
const region            = process.env.AWS_REGION ?? 'us-east-1';

// ─── Load profile & progress ──────────────────────────────────────────────────

const profile = loadProfile();
if (!profile) { console.error('profile.json not found'); process.exit(1); }

let progress = { scraped_cities: [] };
if (existsSync(PROGRESS_FILE)) {
  try { progress = JSON.parse(readFileSync(PROGRESS_FILE, 'utf-8')); } catch {}
}

const allCities = profile.icp?.locations ?? [];
let unscraped   = allCities.filter(c => !progress.scraped_cities.includes(c));

if (unscraped.length === 0) {
  console.log('All cities scraped — resetting rotation and starting over.\n');
  progress.scraped_cities = [];
  unscraped = [...allCities];
  writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));
}

const citiesToScrape = unscraped.slice(0, CITIES_PER_RUN);

// ─── Banner ───────────────────────────────────────────────────────────────────

const now = new Date().toLocaleString('en-US', { timeZone: 'America/New_York' });
console.log('\n╔══════════════════════════════════════════════════════════════╗');
console.log('║              Swarm Daily Outreach Runner                    ║');
console.log(`║  ${now.padEnd(60)}║`);
console.log(`║  Cities to scrape : ${String(citiesToScrape.length).padEnd(41)}║`);
console.log(`║  Send target      : ${String(DAILY_SEND_TARGET).padEnd(41)}║`);
console.log(`║  Cities remaining : ${String(unscraped.length).padEnd(41)}║`);
console.log('╚══════════════════════════════════════════════════════════════╝\n');

// ─── Phase 1: Scrape fresh cities ─────────────────────────────────────────────

let totalLeadsFound = 0;

for (const city of citiesToScrape) {
  console.log(`\n▶  Scraping ${city}...`);

  const scraperCfg = {
    ...scraperNodeConfig(profile),
    node_id:             `daily-scraper-${city.replace(/[^a-z]/gi, '-').toLowerCase()}`,
    allow_self_destruct: false,
    target_locations:    [city],
    daily_cap_per_city:  LEADS_PER_CITY,
    leads_per_cycle:     LEADS_PER_CITY,
    sources:             ['yellowpages', 'web_scrape'],
    hunter_api_key:      '',
    apollo_api_key:      process.env.APOLLO_API_KEY ?? '',
  };

  const scraper    = new ScraperNode(scraperCfg, region);
  let   cityStored = 0;
  const origUpsert = scraper.memory.upsertLead.bind(scraper.memory);

  scraper.memory.upsertLead = async (lead) => {
    const result = await origUpsert(lead);
    if (lead.source_node) {
      cityStored++;
      if (lead.email) {
        process.stdout.write(`  + ${(lead.company ?? lead.website ?? '?').slice(0, 45).padEnd(45)} ${lead.email}\n`);
      }
    }
    return result;
  };

  try {
    await scraper._scrapeAndStore();
  } finally {
    if (scraper._browser) await scraper._browser.close().catch(() => {});
  }

  console.log(`\n  ${city}: ${cityStored} leads stored`);
  totalLeadsFound += cityStored;

  progress.scraped_cities.push(city);
  writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));
}

console.log(`\n${'─'.repeat(64)}`);
console.log(`  Scraping complete — ${totalLeadsFound} new leads stored across ${citiesToScrape.length} cities`);
console.log(`${'─'.repeat(64)}\n`);

// ─── Phase 2: Send 290 emails from full accumulated database ──────────────────

console.log(`▶  Seeding leads and sending up to ${DAILY_SEND_TARGET} emails...\n`);

const emailCfg = {
  ...emailNodeConfig(profile),
  node_id:            'daily-emailer',
  allow_self_destruct: false,
  daily_send_limit:   DAILY_SEND_TARGET,
  from_email:         process.env.SES_FROM_EMAIL ?? 'derek@heinrichstech.com',
  from_name:          process.env.SES_FROM_NAME  ?? 'Derek',
  reply_to:           process.env.SES_REPLY_TO   ?? 'derek@heinrichstech.com',
  feedback_queue_url: process.env.SWARM_SES_FEEDBACK_QUEUE_URL ?? '',
};

const emailer = new EmailNode(emailCfg, region);
emailer._isWithinSendWindow = () => true;  // bypass time window — we decide when to run

let totalSent = 0;
const origSend = emailer.transporter.sendMail.bind(emailer.transporter);

emailer.transporter.sendMail = async (opts) => {
  totalSent++;
  const bodyPreview = (opts.text ?? '').replace(/\s+/g, ' ').slice(0, 120);
  console.log(`  [${String(totalSent).padStart(3)}] → ${opts.to}`);
  console.log(`       Sub: ${opts.subject}`);
  console.log(`       ${bodyPreview}...\n`);
  return origSend(opts);
};

await emailer._seedNewLeads();
await emailer._processPendingSequences();

// ─── Summary ──────────────────────────────────────────────────────────────────

console.log('\n' + '═'.repeat(64));
console.log(`\n  DONE`);
console.log(`  Leads scraped today : ${totalLeadsFound}`);
console.log(`  Emails sent today   : ${totalSent} / ${DAILY_SEND_TARGET}`);
console.log(`  Cities remaining    : ${allCities.length - progress.scraped_cities.length} of ${allCities.length}`);

if (totalSent < DAILY_SEND_TARGET) {
  const deficit = DAILY_SEND_TARGET - totalSent;
  console.log(`\n  ⚠  Only ${totalSent} sent (${deficit} short of target).`);
  console.log(`     Database needs more leads with emails.`);
  console.log(`     Increase CITIES_PER_RUN or add Apollo enrichment.`);
}

console.log('\n  Run again tomorrow:  node run_daily.mjs\n');
process.exit(0);
