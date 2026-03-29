/**
 * seed_sequences.mjs — stamps all unsent leads as sequence pending so the
 * email node picks them up immediately.
 */
import 'dotenv/config';
import { SharedMemory } from './core/shared_memory.mjs';

const memory = new SharedMemory(process.env.AWS_REGION ?? 'us-east-1');

const leads = await memory.getLeads(null, 1000);
const unsent = leads.filter(l =>
  l.email &&
  !l.unsubscribed &&
  !l.bounced &&
  !l.complained &&
  !l.sequence_state,
);

console.log(`Found ${leads.length} total leads, ${unsent.length} unsent.`);

let queued = 0;
for (const lead of unsent) {
  await memory.upsertLead({
    ...lead,
    sequence_state: {
      lead_id:      lead.lead_id,
      campaign_id:  'manual-seed',
      step:         0,
      next_send_at: Math.floor(Date.now() / 1000),
      status:       'pending',
    },
  });
  queued++;
}

console.log(`Queued ${queued} leads for immediate sending.`);
