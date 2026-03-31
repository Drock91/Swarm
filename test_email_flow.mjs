/**
 * test_email_flow.mjs
 *
 * Inserts a fake lead and lets you trigger every scenario manually.
 *
 * Usage:
 *   node test_email_flow.mjs insert       — add test lead, sequence starts immediately
 *   node test_email_flow.mjs advance      — fast-forward next_send_at so step 2/3 fire now
 *   node test_email_flow.mjs status       — show current sequence state of the test lead
 *   node test_email_flow.mjs unsub        — simulate unsubscribe webhook from Brevo
 *   node test_email_flow.mjs bounce       — simulate hard bounce
 *   node test_email_flow.mjs open         — simulate open event
 *   node test_email_flow.mjs click        — simulate click event
 *   node test_email_flow.mjs reply        — simulate human reply (pauses sequence)
 *   node test_email_flow.mjs reply-stop   — simulate reply with STOP (opt-out)
 *   node test_email_flow.mjs delete       — remove test lead entirely
 */

import 'dotenv/config';
import { DynamoDBClient, GetItemCommand } from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall }           from '@aws-sdk/util-dynamodb';
import { SharedMemory }  from './core/shared_memory.mjs';
import { EmailNode }     from './nodes/email_node.mjs';
import { loadProfile, mergeProfileConfig } from './core/profile.mjs';

const TEST_EMAIL   = 'mrheinrichs12@gmail.com';
const TEST_LEAD_ID = 'test-lead-derek-001';
const REGION       = process.env.AWS_REGION ?? 'us-east-1';

const memory = new SharedMemory(REGION);
const ddb    = new DynamoDBClient({ region: REGION });

const profile = loadProfile();
const config  = mergeProfileConfig('email_node', profile, {
  node_id: 'test-runner',
  from_email: process.env.SES_FROM_EMAIL,
  from_name:  process.env.SES_FROM_NAME ?? 'Derek',
});
const emailNode = new EmailNode(config, REGION);

const cmd = process.argv[2];

async function getTestLead() {
  const resp = await ddb.send(new GetItemCommand({
    TableName: 'swarm-leads',
    Key: marshall({ lead_id: TEST_LEAD_ID }),
  }));
  return resp.Item ? unmarshall(resp.Item) : null;
}

function printLead(lead) {
  if (!lead) { console.log('  (no test lead found)'); return; }
  const seq = lead.sequence_state ?? {};
  console.log(`  lead_id      : ${lead.lead_id}`);
  console.log(`  email        : ${lead.email}`);
  console.log(`  company      : ${lead.company}`);
  console.log(`  score        : ${lead.lead_score}`);
  console.log(`  seq step     : ${seq.step ?? 0}`);
  console.log(`  seq status   : ${seq.status ?? 'none'}`);
  console.log(`  next_send_at : ${seq.next_send_at ? new Date(seq.next_send_at * 1000).toLocaleString() : 'not set'}`);
  console.log(`  unsubscribed : ${lead.unsubscribed ?? false}`);
  console.log(`  bounced      : ${lead.bounced ?? false}`);
  console.log(`  replied_human: ${lead.replied_human ?? false}`);
  if (seq.message_ids?.length) console.log(`  message_ids  : ${seq.message_ids.join(', ')}`);
  if (seq.original_subject)    console.log(`  subject      : ${seq.original_subject}`);
}

switch (cmd) {

  case 'insert': {
    console.log(`\nInserting test lead: ${TEST_EMAIL}\n`);
    await memory.upsertLead({
      lead_id:              TEST_LEAD_ID,
      email:                TEST_EMAIL,
      company:              'Derek Test HVAC',
      first_name:           'Derek',
      industry:             'HVAC & Plumbing Contractors',
      location:             'Tampa, FL',
      website:              'https://derektesthvac.com',
      site_tagline:         'Fast, Reliable HVAC Service',
      about_text:           'Family-owned HVAC company serving Tampa Bay since 2008. We specialize in residential AC repair, installation, and maintenance.',
      business_hours:       'Mon-Fri 8am-5pm',
      has_after_hours_gap:  true,
      has_contact_form:     false,
      has_booking_widget:   false,
      site_platform:        'wordpress',
      rating:               4.8,
      review_count:         62,
      lead_score:           93,
      confidence:           'scraped',
      source:               'test',
      sequence_state: {
        lead_id:      TEST_LEAD_ID,
        step:         0,
        next_send_at: Math.floor(Date.now() / 1000), // due immediately
        status:       'pending',
      },
    });
    console.log('Done. The email node will pick this up on its next cycle and send step 1.');
    console.log(`Run: node test_email_flow.mjs status   — to watch progress`);
    break;
  }

  case 'advance': {
    const lead = await getTestLead();
    if (!lead) { console.log('No test lead found. Run: insert first.'); break; }
    const seq = lead.sequence_state ?? {};
    await memory.upsertLead({
      ...lead,
      sequence_state: { ...seq, next_send_at: Math.floor(Date.now() / 1000), status: 'pending' },
    });
    console.log(`Advanced next_send_at to NOW — step ${seq.step ?? 0} will fire on the next email node cycle.`);
    break;
  }

  case 'status': {
    const lead = await getTestLead();
    console.log('\nTest lead status:\n');
    printLead(lead);
    break;
  }

  case 'open': {
    console.log('\nSimulating Brevo "opened" event...');
    await emailNode.handleBrevoEvent({ event: 'opened', email: TEST_EMAIL });
    const lead = await getTestLead();
    console.log(`last_opened_at: ${lead?.last_opened_at ?? 'not set'}`);
    break;
  }

  case 'click': {
    console.log('\nSimulating Brevo "click" event...');
    await emailNode.handleBrevoEvent({ event: 'click', email: TEST_EMAIL, url: 'https://heinrichstech.com' });
    const lead = await getTestLead();
    console.log(`last_clicked_at: ${lead?.last_clicked_at ?? 'not set'}`);
    break;
  }

  case 'unsub': {
    console.log('\nSimulating Brevo "unsubscribe" event...');
    await emailNode.handleBrevoEvent({ event: 'unsubscribe', email: TEST_EMAIL });
    const lead = await getTestLead();
    console.log(`unsubscribed: ${lead?.unsubscribed}`);
    console.log(`sequence_status: ${lead?.sequence_status}`);
    break;
  }

  case 'bounce': {
    console.log('\nSimulating Brevo hard bounce...');
    await emailNode.handleBrevoEvent({ event: 'hard_bounce', email: TEST_EMAIL });
    const lead = await getTestLead();
    console.log(`bounced: ${lead?.bounced}`);
    console.log(`sequence_status: ${lead?.sequence_status}`);
    break;
  }

  case 'reply': {
    console.log('\nSimulating human reply (sequence should pause)...');
    await emailNode.processInboundReply(
      TEST_EMAIL,
      'Re: Quick question about your website',
      'Hey Derek, this actually sounds interesting. Can you tell me more about how the chatbot works?',
      TEST_LEAD_ID,
    );
    const lead = await getTestLead();
    console.log(`replied_human: ${lead?.replied_human}`);
    console.log(`sequence_status: ${lead?.sequence_state?.status}`);
    console.log(`reply_snippet: ${lead?.reply_snippet}`);
    break;
  }

  case 'reply-stop': {
    console.log('\nSimulating reply with STOP (opt-out)...');
    await emailNode.processInboundReply(
      TEST_EMAIL,
      'Re: Quick question about your website',
      'Stop emailing me please.',
      TEST_LEAD_ID,
    );
    const lead = await getTestLead();
    console.log(`unsubscribed: ${lead?.unsubscribed}`);
    console.log(`sequence_status: ${lead?.sequence_status}`);
    break;
  }

  case 'delete': {
    console.log('\nDeleting test lead...');
    // Suppress it so it never gets picked up again
    await memory.suppressLead(TEST_LEAD_ID, 'unsubscribed');
    console.log('Done. Lead suppressed.');
    break;
  }

  default: {
    console.log(`
Usage:
  node test_email_flow.mjs insert      — add test lead (sends step 1 immediately)
  node test_email_flow.mjs status      — show sequence state
  node test_email_flow.mjs advance     — fast-forward to trigger next step now
  node test_email_flow.mjs open        — simulate Brevo open event
  node test_email_flow.mjs click       — simulate Brevo click event
  node test_email_flow.mjs unsub       — simulate unsubscribe
  node test_email_flow.mjs bounce      — simulate hard bounce
  node test_email_flow.mjs reply       — simulate human reply (pauses sequence)
  node test_email_flow.mjs reply-stop  — simulate reply with STOP (opt-out)
  node test_email_flow.mjs delete      — clean up test lead
    `);
  }
}

process.exit(0);
