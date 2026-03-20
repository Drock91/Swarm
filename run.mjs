/**
 * run.mjs — Start any individual Swarm node.
 *
 * Usage:
 *   node run.mjs <node_type> [config_override_json]
 *
 * Examples:
 *   node run.mjs email_node
 *   node run.mjs voice_node '{"daily_call_limit": 100}'
 *   NODE_TYPE=scraper_node NODE_CONFIG='{}' node run.mjs
 *
 * Config is loaded from:
 *   1. Environment variable NODE_CONFIG (JSON string)
 *   2. .env file (SWARM_* vars)
 *   3. CLI argument (second arg, JSON string)
 */

import 'dotenv/config';
import { createNode }                      from './nodes/index.mjs';
import { log }                             from './core/logger.mjs';
import { loadProfile, mergeProfileConfig } from './core/profile.mjs';

const nodeType = process.argv[2] ?? process.env.NODE_TYPE;
if (!nodeType) {
  console.error('Usage: node run.mjs <node_type> [config_json]');
  process.exit(1);
}

let config = {};

// From environment (ECS/Fargate injects this)
if (process.env.NODE_CONFIG) {
  try { config = JSON.parse(process.env.NODE_CONFIG); } catch {}
}

// CLI override (merge on top)
if (process.argv[3]) {
  try { Object.assign(config, JSON.parse(process.argv[3])); } catch {}
}

// Inject env-based credentials if not already in config
const envMap = {
  openai_api_key:        'OPENAI_API_KEY',
  from_email:            'SES_FROM_EMAIL',
  from_name:             'SES_FROM_NAME',
  reply_to:              'SES_REPLY_TO',
  feedback_queue_url:    'SWARM_SES_FEEDBACK_QUEUE_URL',
  apollo_api_key:        'APOLLO_API_KEY',
  hunter_api_key:        'HUNTER_API_KEY',
  serp_api_key:          'SERPAPI_API_KEY',
  queue_url:             `SWARM_${nodeType.replace('_node', '').toUpperCase()}_QUEUE_URL`,
  webhook_base_url:      'SWARM_WEBHOOK_BASE_URL',
};
for (const [key, envKey] of Object.entries(envMap)) {
  if (!config[key] && process.env[envKey]) config[key] = process.env[envKey];
}

// Merge profile-derived defaults (profile fills gaps, env/CLI values always win)
const profile = loadProfile();
if (profile) {
  config = mergeProfileConfig(nodeType, profile, config);
}

const region = process.env.AWS_REGION ?? 'us-east-1';

log.info({ event: 'node_launch', node_type: nodeType, region, has_profile: !!profile });

const node = createNode(nodeType, config, region);
await node.start();
