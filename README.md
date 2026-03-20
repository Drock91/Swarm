# The Swarm 🤖

Autonomous multi-node marketing system running on AWS Fargate.
A Commander Agent orchestrates seven specialized AI nodes that self-improve every 24 hours and share winning strategies with each other.

## Architecture

```
                        ┌─────────────────────┐
                        │   Commander Agent   │
                        │    (swarm.mjs)      │
                        └────────┬────────────┘
                                 │ ECS RunTask + SQS
              ┌──────────────────┼──────────────────┐
              │          ┌───────┴───────┐           │
        ┌─────▼─────┐  ┌─▼────────┐  ┌──▼──────┐  ┌─▼────────┐
        │ EmailNode │  │ SEONode  │  │  DMNode │  │VoiceNode │
        └─────┬─────┘  └─┬────────┘  └──┬──────┘  └─┬────────┘
              │           │              │             │
        ┌─────▼─────────────────────────────────────▼─────┐
        │   ContentNode   ScraperNode   AnalyticsNode     │
        └────────────────────┬──────────────────────────────┘
                             │
                    ┌────────▼────────┐
                    │  SharedMemory   │
                    │ (DynamoDB + S3) │
                    └─────────────────┘
```

## Node Types

| Node | Purpose |
|------|---------|
| **EmailNode** | Cold email sequences via SendGrid, reply detection, A/B subjects |
| **SEONode** | Keyword research (SerpAPI), GPT-4o articles, WordPress publishing |
| **DMNode** | Direct messages on X, LinkedIn, Reddit |
| **VoiceNode** | AI cold calls via Twilio + ElevenLabs TTS |
| **ContentNode** | Social posts — X threads, LinkedIn, Reddit |
| **ScraperNode** | Lead generation — Apollo.io, Hunter.io, Reddit |
| **AnalyticsNode** | Aggregate metrics, LLM insights, Slack reports |

## Swarm Intelligence

- Every 24 hours each node runs a **self-improvement cycle** using GPT-4o to analyze its own performance and evolve its config.
- Nodes that score above **80%** automatically broadcast their winning patterns to the swarm.
- All other nodes absorb those patterns on their next improvement cycle.

## Quick Start

### 1. Prerequisites

- Node.js ≥ 20
- AWS CLI configured (`aws configure`)
- All keys in `.env` (see below)

### 2. Install

```bash
npm install
```

### 3. Configure

```bash
cp .env.example .env
# Edit .env with your API keys
```

### 4. Deploy AWS Infrastructure

```bash
node infra/deploy.mjs
```

Creates all DynamoDB tables, S3 buckets, SQS queues, and ECS cluster.

### 5. Launch the Swarm

```bash
npm start            # starts Commander (orchestrates all nodes)
```

Or run individual nodes locally for testing:

```bash
node run.mjs scraper_node    # start scraper
node run.mjs email_node      # start email sequences
node run.mjs analytics_node  # start analytics
```

### 6. Watch the Dashboard

```bash
npm run status       # one-shot status print
npm run dashboard    # live refresh every 30s
```

Shows:
- Every running node with type, status, generation, uptime
- Per-node: leads found, emails sent, DMs sent, calls made, human replies, reply rate, errors
- Total pipeline summary
- Estimated AWS session cost
- Active campaigns

## npm Scripts

| Script | Description |
|--------|-------------|
| `npm start` | Launch Commander Agent |
| `npm run status` | One-shot dashboard print |
| `npm run dashboard` | Live dashboard (30s refresh) |
| `npm run deploy` | Create all AWS resources |
| `npm run teardown` | Destroy all AWS resources |

## Project Structure

```
The-Swarm/
├── core/
│   ├── base_node.mjs          # Abstract base class for all nodes
│   ├── commander.mjs          # Orchestrator — scales nodes, manages campaigns
│   ├── logger.mjs             # Pino logger singleton
│   ├── self_improvement.mjs   # 24h LLM optimization loop
│   ├── shared_memory.mjs      # DynamoDB + S3 collective brain
│   ├── swarm_intelligence.mjs # Cross-node knowledge sharing
│   └── index.mjs              # Barrel exports
├── nodes/
│   ├── email_node.mjs
│   ├── seo_node.mjs
│   ├── dm_node.mjs
│   ├── voice_node.mjs
│   ├── content_node.mjs
│   ├── scraper_node.mjs
│   ├── analytics_node.mjs
│   └── index.mjs              # createNode factory
├── infra/
│   ├── deploy.mjs             # Bootstrap AWS resources
│   └── teardown.mjs           # Destroy AWS resources
├── run.mjs                    # Start any node type
├── swarm.mjs                  # Start Commander
├── status.mjs                 # CLI dashboard
├── package.json
└── .env.example
```

## AWS Resources

| Type | Name |
|------|------|
| DynamoDB | swarm-nodes, swarm-leads, swarm-campaigns, swarm-metrics, swarm-knowledge, swarm-calls, swarm-costs |
| S3 | swarm-content-store, swarm-exports-store, swarm-models-store |
| SQS | swarm-{type}-queue × 8 |
| ECS | swarm-cluster (Fargate + Fargate Spot) |

## Teardown

```bash
node infra/teardown.mjs        # interactive confirmation
node infra/teardown.mjs --confirm  # skip prompt
```

## License

MIT
