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
import { createNode } from './nodes/index.mjs';
import { log }        from './core/logger.mjs';

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
  sendgrid_api_key:      'SENDGRID_API_KEY',
  openai_api_key:        'OPENAI_API_KEY',
  twilio_account_sid:    'TWILIO_ACCOUNT_SID',
  twilio_auth_token:     'TWILIO_AUTH_TOKEN',
  elevenlabs_api_key:    'ELEVENLABS_API_KEY',
  x_bearer_token:        'X_BEARER_TOKEN',
  linkedin_token:        'LINKEDIN_TOKEN',
  apollo_api_key:        'APOLLO_API_KEY',
  hunter_api_key:        'HUNTER_API_KEY',
  serp_api_key:          'SERP_API_KEY',
  queue_url:             `SWARM_${nodeType.toUpperCase()}_QUEUE_URL`,
  webhook_base_url:      'SWARM_WEBHOOK_BASE_URL',
};
for (const [key, envKey] of Object.entries(envMap)) {
  if (!config[key] && process.env[envKey]) config[key] = process.env[envKey];
}

if (process.env.OPENAI_API_KEY) process.env.OPENAI_API_KEY = process.env.OPENAI_API_KEY;

const region = process.env.AWS_REGION ?? 'us-east-1';

log.info({ event: 'node_launch', node_type: nodeType, region });

const node = createNode(nodeType, config, region);
await node.start();
