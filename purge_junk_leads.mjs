/**
 * purge_junk_leads.mjs — Remove non-local-business leads from DynamoDB.
 * Run once after a bad scrape, then delete this file.
 *
 * Usage: node purge_junk_leads.mjs
 */

import 'dotenv/config';
import { SharedMemory } from './core/shared_memory.mjs';

const JUNK_DOMAINS = [
  'reuters.com', 'cnbc.com', 'bloomberg.com', 'forbes.com', 'wsj.com',
  'deltadental.com', 'anthem.com', 'mutualofomaha.com', 'cigna.com', 'aetna.com',
  'delta.org', 'thomsonreuters.com',
];

const memory = new SharedMemory(process.env.AWS_REGION ?? 'us-east-1');

const all     = await memory.getLeads(null, 1000);
const junk    = all.filter(l => {
  const domain = (l.website ?? l.email?.split('@')[1] ?? '').toLowerCase();
  const isGov  = domain.endsWith('.gov') || domain.endsWith('.mil') || domain.endsWith('.edu');
  return isGov || JUNK_DOMAINS.some(d => domain.includes(d));
});

if (!junk.length) {
  console.log('No junk leads found.');
  process.exit(0);
}

console.log(`Found ${junk.length} junk lead(s) to purge:\n`);
for (const lead of junk) {
  console.log(`  [${lead.lead_id}] ${lead.email ?? '(no email)'}  —  ${lead.website ?? '?'}`);
  // Use 'unsubscribed' so _seedNewLeads correctly filters this lead out forever
  await memory.suppressLead(lead.lead_id, 'unsubscribed');
}

console.log(`\nDone. ${junk.length} lead(s) suppressed.`);
process.exit(0);
