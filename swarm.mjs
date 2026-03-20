/**
 * swarm.mjs — Launch the Commander Agent.
 *
 * Usage:
 *   node swarm.mjs
 *   npm start
 *
 * The Commander reads from .env and AWS credentials,
 * then starts orchestrating all nodes automatically.
 */

import 'dotenv/config';
import { CommanderAgent }            from './core/commander.mjs';
import { log }                       from './core/logger.mjs';
import { loadProfile, bootstrapGoals } from './core/profile.mjs';

const region = process.env.AWS_REGION ?? 'us-east-1';

// Build node_queues map from env vars
const nodeTypes = ['email_node', 'seo_node', 'scraper_node', 'analytics_node'];
const nodeQueues = {};
const taskDefs   = {};
for (const t of nodeTypes) {
  const qKey = `SWARM_${t.toUpperCase()}_QUEUE_URL`;
  const dKey  = `SWARM_${t.toUpperCase()}_TASK_DEF`;
  if (process.env[qKey]) nodeQueues[t] = process.env[qKey];
  if (process.env[dKey]) taskDefs[t]   = process.env[dKey];
}

const config = {
  node_queues:             nodeQueues,
  task_definitions:        taskDefs,
  ecs_cluster:             process.env.SWARM_ECS_CLUSTER ?? 'swarm-cluster',
  orchestration_interval:  parseInt(process.env.SWARM_ORCHESTRATION_INTERVAL ?? '300'),
  llm_model:               process.env.SWARM_LLM_MODEL ?? 'gpt-4o',
  network_config:          process.env.SWARM_NETWORK_CONFIG
    ? JSON.parse(process.env.SWARM_NETWORK_CONFIG)
    : {},
};

// Load business profile — drives campaigns, targeting, and LLM context
const profile = loadProfile();
if (profile) {
  config.business_context = [
    profile.business?.name,
    profile.business?.description,
    profile.offer?.primary ? `Offer: ${profile.offer.primary.name} @ ${profile.offer.primary.price}` : null,
  ].filter(Boolean).join(' | ');
}

log.info({ event: 'commander_launch', region, node_queues: Object.keys(nodeQueues), has_profile: !!profile });

const commander = new CommanderAgent(config, region);

// Bootstrap campaigns — prefer profile.campaign_goals, fall back to env var
const goals = profile
  ? bootstrapGoals(profile)
  : process.env.SWARM_BOOTSTRAP_GOALS
    ? (() => { try { return JSON.parse(process.env.SWARM_BOOTSTRAP_GOALS); } catch { return []; } })()
    : [];

if (goals.length) {
  log.info({ event: 'bootstrap_goals', count: goals.length, source: profile ? 'profile.json' : 'env' });
  await commander.bootstrapInitialCampaigns(goals);
}

await commander.start();
