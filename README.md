# The Swarm

Autonomous multi-node marketing system. Specialized AI agents that scrape leads, send cold email sequences, generate SEO content, and self-improve every 24 hours — all coordinated by a Commander Agent.

Production target is AWS ECS Fargate. Local dev uses Docker Compose.

---

## Architecture

```
Commander (swarm.mjs)
  └─ reads metrics from SharedMemory (DynamoDB + S3)
  └─ sends tasks via SQS to individual node queues
  └─ spawns/kills nodes dynamically (ECS RunTask or dockerode)

Nodes (nodes/*.mjs)
  └─ extend BaseNode (core/base_node.mjs)
  └─ consume SQS tasks, report heartbeat + metrics to SharedMemory
  └─ run 24h self-improvement via SelfImprovementEngine (LLM config diff)
  └─ broadcast winning patterns (score >= 80%) via SwarmIntelligence
  └─ self-destruct if score < threshold for 3 cycles
```

---

## Nodes

| Node | Purpose |
|------|---------|
| `free_scraper_node` | 100% free lead generation — Bing + BBB + Google Maps via Puppeteer stealth. Extracts email, phone, owner name, business hours, after-hours gap, chatbot detection, site platform, lead score. No paid APIs required. |
| `scraper_node` | Lead generation with paid APIs — Hunter.io Discover, Apollo.io, YellowPages. |
| `email_node` | Cold email via AWS SES — multi-step drip sequences, CAN-SPAM compliant, bounce/complaint auto-suppression, send window enforcement. |
| `seo_node` | SEO content — GPT-4o articles, keyword research via SerpAPI, publishes to WordPress or S3. |
| `analytics_node` | Aggregates cross-node metrics, writes daily S3 reports, surfaces anomalies to Commander. |

---

## Lead Scraper (Free — No API Keys Needed)

The `FreeScraperNode` is a standalone lead generation pipeline that runs entirely on Puppeteer. No Hunter, Apollo, or any paid API required.

### Sources

- **Bing Search** — queries local business results, extracts domain from `<cite>` element (bypasses redirect URLs)
- **BBB** — Better Business Bureau local search, scoped to main results to avoid sidebar contamination
- **Google Maps** — scrolls the Places feed, visits each listing to extract website, rating, review count, and hours

### What It Captures Per Lead

| Field | Description |
|-------|-------------|
| `email` | Scraped from site → WHOIS/RDAP → pattern guess + MX check |
| `first_name` | Owner name extracted from page text and headings |
| `phone` | From page text |
| `business_hours` | Structured hours from tables, dl/dt, or inline text |
| `has_after_hours_gap` | True if closed evenings/weekends (core pitch for AI chatbot) |
| `has_contact_form` | True if site has a form |
| `has_booking_widget` | Detects Calendly, Acuity, Square, etc. |
| `has_chatbot` | Detects 30+ chatbot platforms — hot leads have this false |
| `site_platform` | WordPress, Wix, Squarespace, Shopify, Webflow, custom |
| `lead_score` | 0–100 priority score for email queue ordering |
| `rating` / `review_count` | From Google Maps listing |

### Lead Scoring (0–100)

| Signal | Points |
|--------|--------|
| Email scraped directly from site | +30 |
| Email from WHOIS | +22 |
| Email guessed (pattern + MX validated) | +12 |
| Has after-hours gap (core pain point) | +25 |
| No contact form (maximum urgency) | +15 |
| Owner name found (personalizable email) | +10 |
| Has phone | +5 |
| Has booking widget (tech-forward upsell) | +5 |
| 10+ reviews | +5 |
| Rating >= 4.0 | +3 |

### Running the Scraper

```bash
# 2 workers, all 160+ US cities divided evenly (safe default for home PC)
node run_free_scrapers.mjs

# 4 workers — faster, covers all cities in 4 parallel slices
node run_free_scrapers.mjs --workers 4

# Test a single region before running overnight
node run_free_scrapers.mjs texas

# Check results
node analyze_leads.mjs
```

**RAM guide:**
- 2 workers → ~800MB RAM (home PC safe)
- 4 workers → ~1.5GB RAM (needs 8GB+ free)
- 8 workers → ~3GB RAM (needs 16GB+ RAM)

---

## Email System

Configured entirely through `profile.json`. No code changes needed to adjust targeting, sequences, or send windows.

```bash
node run.mjs email_node      # start the email node
node run_daily.mjs           # one daily send cycle
```

See `my-workspace/email-profile.json` for a ready-to-use email campaign profile template with all variables documented.

### CAN-SPAM Compliance (built-in)
- Unsubscribe footer + `List-Unsubscribe` header on every email
- Bounce and complaint auto-suppression via SES → SNS → SQS feedback loop
- Opt-out keyword detection in replies
- Send window enforcement (configurable in `profile.json`)
- Rate monitoring with auto-pause

---

## Quick Start

### 1. Prerequisites

- Node.js >= 20
- AWS CLI configured (`aws configure`)
- All keys in `.env` (copy from `.env.example`)

### 2. Install

```bash
npm install
```

### 3. Configure

```bash
cp profile.example.json profile.json
# Edit profile.json with your business info, offer, ICP, and email sequences
```

### 4. Deploy AWS Infrastructure

```bash
node infra/deploy.mjs
```

Creates all DynamoDB tables, S3 buckets, SQS queues, and ECS cluster.

### 5. Scrape Leads

```bash
node run_free_scrapers.mjs --workers 2
```

### 6. Send Emails

```bash
node run.mjs email_node
```

### 7. Watch the Dashboard

```bash
npm run status       # one-shot print
npm run dashboard    # live refresh every 30s
```

---

## Configuration

All targeting, email sequences, SEO keywords, and campaign goals live in `profile.json`. Edit it to change the offer, ICP, send windows, follow-up cadence, or budget — without touching any node code.

See `profile.example.json` for a fully-documented template.

---

## Key Environment Variables

| Variable | Used By |
|----------|---------|
| `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | All nodes |
| `OPENAI_API_KEY` | Self-improvement, SEO content, swarm synthesis |
| `SES_FROM_EMAIL`, `SES_FROM_NAME`, `SES_REPLY_TO` | email_node |
| `HUNTER_API_KEY`, `APOLLO_API_KEY` | scraper_node (optional — free scraper needs neither) |
| `SERPAPI_API_KEY` | seo_node |
| `WP_BASE_URL`, `WP_USERNAME`, `WP_APP_PASSWORD` | seo_node (WordPress publishing) |
| `SLACK_WEBHOOK_URL` | analytics_node (optional) |

---

## AWS Resources

| Type | Name |
|------|------|
| DynamoDB | swarm-nodes, swarm-leads, swarm-campaigns, swarm-metrics, swarm-knowledge, swarm-calls, swarm-costs |
| S3 | swarm-content-store, swarm-exports-store, swarm-models-store |
| SQS | swarm-{type}-queue x 8 |
| ECS | swarm-cluster (Fargate + Fargate Spot) |

```bash
node infra/deploy.mjs           # create all resources
node infra/teardown.mjs         # destroy all resources (prompts for confirmation)
node infra/teardown.mjs --confirm  # skip prompt
```

---

## Project Structure

```
The-Swarm/
├── core/
│   ├── base_node.mjs          # Abstract base class for all nodes
│   ├── commander.mjs          # Orchestrator — scales nodes, manages campaigns
│   ├── shared_memory.mjs      # DynamoDB + S3 collective brain
│   ├── self_improvement.mjs   # 24h LLM optimization loop
│   ├── swarm_intelligence.mjs # Cross-node knowledge sharing
│   ├── profile.mjs            # Loads profile.json business context
│   └── logger.mjs             # Pino logger singleton
├── nodes/
│   ├── free_scraper_node.mjs  # Free lead gen — Bing + BBB + Maps
│   ├── scraper_node.mjs       # Paid lead gen — Hunter + Apollo + YP
│   ├── email_node.mjs         # Cold email — SES + drip sequences
│   ├── seo_node.mjs           # SEO content + WordPress publishing
│   └── analytics_node.mjs     # Metrics + anomaly detection
├── infra/
│   ├── deploy.mjs             # Bootstrap AWS resources
│   └── teardown.mjs           # Destroy AWS resources
├── run_free_scrapers.mjs      # Launch N parallel free scraper workers
├── analyze_leads.mjs          # Print lead database stats
├── run.mjs                    # Start any single node
├── swarm.mjs                  # Start Commander
├── status.mjs                 # CLI dashboard
├── profile.json               # Your business config (gitignored)
├── profile.example.json       # Template — copy to profile.json
└── .env.example               # All required env vars documented
```

---

## Adding a New Node

1. Create `nodes/[name]_node.mjs` extending `BaseNode`
2. Implement `runCycle()`, `collectMetrics()`, `getImprovementContext()`
3. Add `SWARM_[NAME]_QUEUE_URL` env var
4. Add service to `docker-compose.yml`
5. Add campaign goals to `profile.json` if applicable

---

## License

MIT
