import { DynamoDBClient, ScanCommand } from '@aws-sdk/client-dynamodb';
import { config } from 'dotenv';
config();

const client = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' });

let items = [];
let lastKey = undefined;

do {
  const resp = await client.send(new ScanCommand({
    TableName: 'swarm-leads',
    ExclusiveStartKey: lastKey,
  }));
  items = items.concat(resp.Items || []);
  lastKey = resp.LastEvaluatedKey;
} while (lastKey);

const total = items.length;
const hasEmail    = items.filter(i => i.email?.S && i.email.S !== 'null');
const noEmail     = items.filter(i => !i.email?.S || i.email.S === 'null');
const hasChatbot  = items.filter(i => i.has_chatbot?.BOOL === true);
const suppressed  = items.filter(i => i.status?.S === 'suppressed');
const reddit      = items.filter(i => (i.source?.S || '').includes('reddit'));
const yellowpages = items.filter(i => i.source?.S === 'yellowpages');
const good        = items.filter(i => i.email?.S && i.email.S !== 'null' && !i.has_chatbot?.BOOL && i.status?.S !== 'suppressed');

const sources    = {};
const industries = {};
const locations  = {};

for (const i of items) {
  const s   = i.source?.S || 'unknown';
  const ind = i.industry?.S;
  const loc = i.location?.S;
  sources[s] = (sources[s] || 0) + 1;
  if (ind) industries[ind] = (industries[ind] || 0) + 1;
  if (loc) locations[loc]  = (locations[loc]  || 0) + 1;
}

const sorted = (obj) => Object.entries(obj).sort((a,b) => b[1]-a[1]);

console.log('=== LEAD DATABASE ANALYSIS ===\n');
console.log(`TOTAL:              ${total}`);
console.log(`Has email:          ${hasEmail.length}`);
console.log(`No email (useless): ${noEmail.length}`);
console.log(`Has chatbot (skip): ${hasChatbot.length}`);
console.log(`Suppressed:         ${suppressed.length}`);
console.log(`Reddit leads:       ${reddit.length}`);
console.log(`Yellowpages leads:  ${yellowpages.length}`);
console.log(`\nACTIONABLE (email + no chatbot + not suppressed): ${good.length}`);

console.log('\n--- BY SOURCE ---');
sorted(sources).forEach(([s,c]) => console.log(`  ${s}: ${c}`));

console.log('\n--- TOP INDUSTRIES ---');
sorted(industries).slice(0,12).forEach(([s,c]) => console.log(`  ${s}: ${c}`));

console.log('\n--- BY CITY ---');
sorted(locations).forEach(([s,c]) => console.log(`  ${s}: ${c}`));

console.log('\n--- SAMPLE GOOD LEADS ---');
good.slice(0,10).forEach(g => {
  console.log(`  ${g.company?.S || '?'} | ${g.email?.S} | ${g.industry?.S || '?'} | ${g.location?.S || '?'} | conf: ${g.confidence?.N || '?'}`);
});

console.log('\n--- BAD / INCOMPLETE LEADS ---');
const bad = items.filter(i => !i.email?.S || i.email.S === 'null');
const badSample = bad.slice(0, 10);
badSample.forEach(b => {
  console.log(`  ${b.company?.S || b.reddit_user?.S || '?'} | source: ${b.source?.S || '?'} | industry: ${b.industry?.S || 'N/A'}`);
});
if (bad.length > 10) console.log(`  ... and ${bad.length - 10} more with no email`);
