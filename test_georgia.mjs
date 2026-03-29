/**
 * test_georgia.mjs — City-hopping pipeline: keeps scraping new cities until 5 emails sent.
 *
 * Usage: node test_georgia.mjs
 */

import 'dotenv/config';
import { ScraperNode }                              from './nodes/scraper_node.mjs';
import { EmailNode }                               from './nodes/email_node.mjs';
import { loadProfile, scraperNodeConfig,
         emailNodeConfig }                         from './core/profile.mjs';
import { log }                                     from './core/logger.mjs';

const EMAIL_TARGET = 5;
const CAP_PER_CITY = 200;
const region       = process.env.AWS_REGION ?? 'us-east-1';
const profile      = loadProfile();

// Cities to try in order — stops as soon as EMAIL_TARGET is reached
const CITY_QUEUE = [
  'Birmingham, AL', 'Huntsville, AL', 'Montgomery, AL', 'Mobile, AL',
  'Tuscaloosa, AL', 'Dothan, AL', 'Auburn, AL', 'Decatur, AL',
];

console.log('\n╔══════════════════════════════════════════════════════╗');
console.log('║   Swarm Pipeline Test — city hop until 5 sent        ║');
console.log(`║   Cities queued: ${String(CITY_QUEUE.length).padEnd(35)}║`);
console.log('╚══════════════════════════════════════════════════════╝\n');

// ── Emailer setup (shared across all city batches) ────────────────────────────
const emailCfg = {
  ...emailNodeConfig(profile),
  node_id:            'test-emailer',
  allow_self_destruct: false,
  daily_send_limit:   EMAIL_TARGET,
  from_email:         process.env.SES_FROM_EMAIL ?? 'derek@heinrichstech.com',
  from_name:          process.env.SES_FROM_NAME  ?? 'Derek',
  reply_to:           process.env.SES_REPLY_TO   ?? 'derek@heinrichstech.com',
  feedback_queue_url: process.env.SWARM_SES_FEEDBACK_QUEUE_URL ?? '',
};

const emailer = new EmailNode(emailCfg, region);
emailer._isWithinSendWindow = () => true;

let totalSent = 0;

// Intercept sends to print full email + track count
const origSend = emailer.transporter.sendMail.bind(emailer.transporter);
emailer.transporter.sendMail = async (opts) => {
  const rawText  = (opts.text ?? opts.html ?? '').replace(/<[^>]+>/g, '');
  const bodyText = rawText.replace(/You are receiving this[\s\S]*$/i, '').replace(/\s+/g, ' ').trim();
  totalSent++;
  console.log(`┌─── EMAIL ${totalSent} of ${EMAIL_TARGET} ${'─'.repeat(38)}`);
  console.log(`│  To      : ${opts.to}`);
  console.log(`│  Subject : ${opts.subject}`);
  console.log(`│  Body    :`);
  const words = bodyText.split(' ');
  let line = '│    ';
  for (const word of words) {
    if (line.length + word.length > 74) { console.log(line); line = '│    ' + word + ' '; }
    else line += word + ' ';
  }
  if (line.trim() !== '│') console.log(line);
  console.log('└' + '─'.repeat(54) + '\n');
  return origSend(opts);
};

// ── City hop loop ─────────────────────────────────────────────────────────────
for (const city of CITY_QUEUE) {
  if (totalSent >= EMAIL_TARGET) break;

  console.log(`\n▶  Scraping ${city}...`);

  const scraperCfg = {
    ...scraperNodeConfig(profile),
    node_id:             `scraper-${city.replace(/[^a-z]/gi, '-').toLowerCase()}`,
    allow_self_destruct: false,
    target_locations:    [city],
    daily_cap_per_city:  CAP_PER_CITY,
    leads_per_cycle:     CAP_PER_CITY,
    sources:             ['yellowpages', 'web_scrape'],   // YP first — far better local results
    hunter_api_key:      '',
    apollo_api_key:      process.env.APOLLO_API_KEY ?? '',
  };

  const scraper = new ScraperNode(scraperCfg, region);
  let cityStored = 0;

  const origUpsert = scraper.memory.upsertLead.bind(scraper.memory);
  scraper.memory.upsertLead = async (lead) => {
    const result = await origUpsert(lead);
    if (lead.source_node) {
      cityStored++;
      console.log(
        `  + ${(lead.company ?? lead.website ?? '?').slice(0, 50)}\n` +
        `    ${lead.industry}  |  email: ${lead.email ?? '(none)'}` +
        (lead.site_tagline ? `\n    "${lead.site_tagline.slice(0, 80)}"` : '') + '\n'
      );
    }
    return result;
  };

  try {
    await scraper._scrapeAndStore();
  } finally {
    if (scraper._browser) await scraper._browser.close().catch(() => {});
  }

  console.log(`  ${city}: ${cityStored} lead(s) stored`);

  if (cityStored === 0) {
    console.log(`  No leads found in ${city}, moving on...\n`);
    continue;
  }

  // Seed new leads and attempt sends
  const remaining = EMAIL_TARGET - totalSent;
  emailer.dailySendLimit = totalSent + remaining;
  await emailer._seedNewLeads();
  await emailer._processPendingSequences();

  const sentThisRound = emailer.getCounter('emails_sent') - (totalSent - emailer.getCounter('emails_sent') + emailer.getCounter('emails_sent'));
  console.log(`  Emails sent so far: ${totalSent} / ${EMAIL_TARGET}`);
}

console.log('\n' + '═'.repeat(56));
console.log(`\n  Done. ${totalSent} email(s) sent across ${CITY_QUEUE.length} cities queued.`);
if (totalSent < EMAIL_TARGET) {
  console.log(`\n  Only ${totalSent} sent — not enough leads with valid emails found.`);
  console.log(`  The scraper found mostly sites with no public email address.`);
  console.log(`  Consider adding Apollo API key or Hunter API key for better coverage.`);
}
console.log('');
process.exit(0);
