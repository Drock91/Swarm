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
import { CommanderAgent } from './core/commander.mjs';
import { log }             from './core/logger.mjs';

const region = process.env.AWS_REGION ?? 'us-east-1';

// Build node_queues map from env vars
const nodeTypes = ['email_node', 'seo_node', 'dm_node', 'voice_node', 'content_node', 'scraper_node', 'analytics_node'];
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

log.info({ event: 'commander_launch', region, node_queues: Object.keys(nodeQueues) });

const commander = new CommanderAgent(config, region);

// Bootstrap initial campaigns if defined
if (process.env.SWARM_BOOTSTRAP_GOALS) {
  try {
    const goals = JSON.parse(process.env.SWARM_BOOTSTRAP_GOALS);
    await commander.bootstrapInitialCampaigns(goals);
  } catch (err) {
    log.warn({ event: 'bootstrap_parse_error', error: err.message });
  }
}

await commander.start();
