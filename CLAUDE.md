# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
npm install                   # Install dependencies
npm start                     # Launch Commander Agent (orchestrator)
node run.mjs <node_type>      # Run a single node (e.g. email_node, scraper_node)
npm run status                # One-shot metrics dashboard
npm run dashboard             # Live dashboard (30s auto-refresh)
npm run deploy                # Create all AWS resources (DynamoDB, S3, SQS, ECS)
npm run teardown              # Destroy all AWS resources (prompts for confirmation)
docker-compose up -d          # Spin up full swarm locally
```

No test or lint scripts exist. `vitest` is installed but unused.

## Architecture

**The Swarm** is an autonomous multi-node marketing system — specialized AI agents that self-improve every 24 hours and share winning strategies across the swarm. Production target is AWS ECS Fargate; local dev uses Docker Compose.

### Data Flow

```
Commander (swarm.mjs)
  └─ reads metrics from SharedMemory (DynamoDB/S3)
  └─ sends tasks via SQS to individual node queues
  └─ spawns/kills nodes dynamically (ECS RunTask or dockerode)

Nodes (nodes/*.mjs)
  └─ extend BaseNode (core/base_node.mjs)
  └─ consume SQS tasks, report heartbeat + metrics to SharedMemory
  └─ run 24h self-improvement via SelfImprovementEngine (LLM config diff)
  └─ broadcast winning patterns (score ≥ 80%) via SwarmIntelligence
  └─ self-destruct if score < threshold for 3 cycles, broadcasting failure knowledge first
```

### Core Layer (`core/`)

| File | Role |
|------|------|
| `base_node.mjs` | Abstract base for all nodes — heartbeat, metrics, improvement loop, SQS polling |
| `commander.mjs` | Orchestrator — scales/kills nodes, allocates budget, triggers improvements |
| `shared_memory.mjs` | DynamoDB + S3 abstraction all nodes use for reads/writes |
| `self_improvement.mjs` | 24h cycle: pull metrics → GPT-4o → apply config diff live |
| `swarm_intelligence.mjs` | Cross-node pattern sharing — absorb winning configs, LLM synthesis for next-gen |
| `profile.mjs` | Loads `profile.json` business context |
| `logger.mjs` | Pino logging singleton |

### Worker Nodes (`nodes/`)

| Node | Purpose |
|------|---------|
| `scraper_node.mjs` | Lead generation — Google+Puppeteer (free), Hunter Discover, Apollo.io; filters businesses that already have chatbots |
| `email_node.mjs` | Cold email via SES — multi-step drip, CAN-SPAM compliant, bounce/complaint auto-suppression, send window enforcement |
| `seo_node.mjs` | SEO content — GPT-4o articles, keyword research (SerpAPI), publishes to WordPress or S3 |
| `analytics_node.mjs` | Aggregates cross-node metrics, writes daily S3 reports, surfaces anomalies to Commander |

### AWS Resources (created by `infra/deploy.mjs`)

- **DynamoDB tables**: `swarm-nodes`, `swarm-leads`, `swarm-campaigns`, `swarm-metrics`, `swarm-knowledge`, `swarm-calls`, `swarm-costs`
- **S3 buckets**: `swarm-content-store`, `swarm-exports-store`, `swarm-models-store`
- **SQS queues**: one per node type (email, seo, scraper, analytics, dm, voice, content, commander)
- **ECS cluster**: `swarm-cluster` (Fargate + Fargate Spot)

### Adding a New Node

1. Create `nodes/[name]_node.mjs` extending `BaseNode`
2. Implement `runCycle()`, `collectMetrics()`, `getImprovementContext()`
3. Export from `nodes/index.mjs` and add to `NODE_REGISTRY`
4. Add `SWARM_[NAME]_QUEUE_URL` env var (matches the pattern in other nodes)
5. Add service to `docker-compose.yml`
6. Add campaign goals to `profile.json` if applicable

## Business Configuration

All campaign targeting, email sequences, SEO keywords, and goals live in `profile.json`. This file drives node behavior — edit it to change the offer, ICP, send windows, follow-up cadence, or budget targets without touching node code.

## Key Environment Variables

| Variable | Used By |
|----------|---------|
| `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | All nodes |
| `OPENAI_API_KEY` | Self-improvement, SEO content, swarm synthesis |
| `SES_FROM_EMAIL`, `SES_FROM_NAME`, `SES_REPLY_TO` | email_node |
| `HUNTER_API_KEY`, `APOLLO_API_KEY` | scraper_node |
| `SERPAPI_API_KEY` | seo_node |
| `WP_BASE_URL`, `WP_USERNAME`, `WP_APP_PASSWORD` | seo_node (WordPress publishing) |
| `SLACK_WEBHOOK_URL` | analytics_node (optional notifications) |

See `.env.example` for the full list.
