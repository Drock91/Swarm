/**
 * CommanderAgent — The orchestrator brain of the Swarm.
 *
 * Responsibilities:
 *   - Sets campaign goals and distributes tasks to worker nodes via SQS
 *   - Reads metrics from SharedMemory every cycle
 *   - Decides which nodes to scale up (spawn more) or kill (underperforming)
 *   - Reallocates rate limits / budget toward best channels
 *   - Triggers the 24-hour improvement cycle across all nodes
 *   - Spawns improved next-generation nodes using SwarmIntelligence synthesis
 */

import { ECSClient, RunTaskCommand } from '@aws-sdk/client-ecs';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import { randomUUID } from 'crypto';
import OpenAI from 'openai';
import { SharedMemory } from './shared_memory.mjs';
import { SwarmIntelligence } from './swarm_intelligence.mjs';
import { log } from './logger.mjs';

const sleep = ms => new Promise(r => setTimeout(r, ms));

const COMMANDER_SYSTEM_PROMPT = `
You are the Commander Agent of an autonomous marketing swarm.
You orchestrate multiple specialized nodes (email, SEO, DM, voice, content, scraper, analytics).

Given current metrics, active campaigns, and node statuses, decide:
1. Which campaigns to prioritize or pause
2. Which nodes to scale up (spawn another instance) or kill
3. How to reallocate budget/rate limits
4. What new campaigns to launch this cycle

Respond ONLY with a JSON object:
{
  "campaigns_to_pause":    ["campaign_id", ...],
  "campaigns_to_launch":   [{ ...campaign config }, ...],
  "nodes_to_kill":         ["node_id", ...],
  "nodes_to_scale":        [{ "node_type": "...", "count": N, "rationale": "..." }],
  "budget_reallocations":  [{ "node_type": "...", "new_daily_budget": N }],
  "commander_notes":       "brief summary of decisions"
}
`.trim();

export class CommanderAgent {
  constructor(config, region = 'us-east-1') {
    this.config          = config;
    this.region          = region;
    this.nodeId          = `commander-${randomUUID().slice(0, 8)}`;
    this.memory          = new SharedMemory(region);
    this.sqs             = new SQSClient({ region });
    this.ecs             = new ECSClient({ region });
    this.openai          = new OpenAI();
    this._running        = false;
    this._swarmIQCache   = {};

    // Maps of node_type → AWS resource
    this.nodeQueues      = config.node_queues ?? {};        // { email_node: "https://sqs..." }
    this.taskDefinitions = config.task_definitions ?? {};   // { email_node: "swarm-email:3" }
    this.ecsCluster      = config.ecs_cluster ?? 'swarm-cluster';

    log.info({ event: 'commander_created', commander_id: this.nodeId });
  }

  // ------------------------------------------------------------------ //
  //  SWARM IQ HELPERS                                                   //
  // ------------------------------------------------------------------ //

  _swarmFor(nodeType) {
    if (!this._swarmIQCache[nodeType]) {
      this._swarmIQCache[nodeType] = new SwarmIntelligence({
        memory:   this.memory,
        nodeType,
      });
    }
    return this._swarmIQCache[nodeType];
  }

  // ------------------------------------------------------------------ //
  //  LIFECYCLE                                                           //
  // ------------------------------------------------------------------ //

  async start() {
    this._running = true;
    log.info({ event: 'commander_starting', commander_id: this.nodeId });

    process.once('SIGINT',  async () => { await this.stop(); process.exit(0); });
    process.once('SIGTERM', async () => { await this.stop(); process.exit(0); });

    await Promise.allSettled([
      this._orchestrationLoop(),
      this._campaignMonitorLoop(),
    ]);
  }

  async stop() {
    this._running = false;
    log.info({ event: 'commander_stopped', commander_id: this.nodeId });
  }

  // ------------------------------------------------------------------ //
  //  ORCHESTRATION LOOP                                                  //
  // ------------------------------------------------------------------ //

  async _orchestrationLoop() {
    const interval = (this.config.orchestration_interval ?? 300) * 1000;
    while (this._running) {
      try {
        await this._runOrchestrationCycle();
      } catch (err) {
        log.error({ event: 'orchestration_error', error: err.message });
      }
      await sleep(interval);
    }
  }

  async _runOrchestrationCycle() {
    const [nodes, campaigns] = await Promise.all([
      this.memory.getAllNodes(),
      this.memory.getActiveCampaigns(),
    ]);
    const metrics  = this._aggregateMetrics(nodes);
    const decision = await this._llmDecide({ nodes, campaigns, metrics });
    if (!decision) return;

    log.info({ event: 'commander_decision', notes: decision.commander_notes ?? '' });

    // Kill underperformers
    for (const nodeId of (decision.nodes_to_kill ?? [])) {
      await this._killNode(nodeId);
    }

    // Scale winning node types
    for (const order of (decision.nodes_to_scale ?? [])) {
      for (let i = 0; i < (order.count ?? 1); i++) {
        await this._spawnNode(order.node_type);
      }
    }

    // Pause campaigns
    for (const cid of (decision.campaigns_to_pause ?? [])) {
      await this.memory.pauseCampaign(cid);
    }

    // Launch new campaigns
    for (const campaignCfg of (decision.campaigns_to_launch ?? [])) {
      const cid = await this.memory.createCampaign(campaignCfg);
      log.info({ event: 'campaign_launched', campaign_id: cid });
      await this._distributeCampaign(campaignCfg);
    }

    // Budget reallocations
    for (const realloc of (decision.budget_reallocations ?? [])) {
      await this._sendBudgetUpdate(realloc.node_type, realloc.new_daily_budget);
    }
  }

  // ------------------------------------------------------------------ //
  //  LLM DECISION-MAKING                                                //
  // ------------------------------------------------------------------ //

  async _llmDecide({ nodes, campaigns, metrics }) {
    const prompt = JSON.stringify({
      timestamp:            new Date().toISOString(),
      active_nodes:         nodes.filter(n => n.status === 'running').length,
      node_breakdown:       this._nodeTypeSummary(nodes),
      active_campaigns:     campaigns.length,
      aggregate_metrics:    metrics,
      underperforming_nodes: this._findUnderperformers(nodes),
      top_performers:       this._findTopPerformers(nodes),
    }, null, 2);

    try {
      const resp = await this.openai.chat.completions.create({
        model:           this.config.llm_model ?? 'gpt-4o',
        messages:        [
          { role: 'system', content: COMMANDER_SYSTEM_PROMPT },
          { role: 'user',   content: prompt },
        ],
        response_format: { type: 'json_object' },
        temperature:     0.2,
      });
      return JSON.parse(resp.choices[0].message.content);
    } catch (err) {
      log.error({ event: 'commander_llm_error', error: err.message });
      return null;
    }
  }

  // ------------------------------------------------------------------ //
  //  NODE MANAGEMENT                                                     //
  // ------------------------------------------------------------------ //

  async _spawnNode(nodeType, baseConfig = {}) {
    const swarmIQ    = this._swarmFor(nodeType);
    const synthConfig = await swarmIQ.synthesizeOptimalConfig(5) ?? {};
    const merged      = { ...synthConfig, ...baseConfig };
    const taskDef     = this.taskDefinitions[nodeType];

    if (!taskDef) {
      log.warn({ event: 'no_task_definition', node_type: nodeType });
      return null;
    }

    try {
      const resp = await this.ecs.send(new RunTaskCommand({
        cluster:              this.ecsCluster,
        taskDefinition:       taskDef,
        launchType:           'FARGATE',
        networkConfiguration: this.config.network_config ?? {},
        overrides: {
          containerOverrides: [{
            name:        nodeType,
            environment: [
              { name: 'NODE_CONFIG', value: JSON.stringify(merged) },
              { name: 'NODE_TYPE',   value: nodeType },
            ],
          }],
        },
      }));
      const taskArn = resp.tasks?.[0]?.taskArn ?? null;
      log.info({ event: 'node_spawned', node_type: nodeType, task_arn: taskArn });
      return taskArn;
    } catch (err) {
      log.error({ event: 'spawn_error', node_type: nodeType, error: err.message });
      return null;
    }
  }

  async _killNode(nodeId) {
    const node = await this.memory.getNode(nodeId);
    if (!node) return;
    const queueUrl = this.nodeQueues[node.node_type];
    if (queueUrl) {
      await this.sqs.send(new SendMessageCommand({
        QueueUrl:    queueUrl,
        MessageBody: JSON.stringify({ command: 'shutdown', target_node_id: nodeId }),
      }));
    }
    await this.memory.deregisterNode(nodeId, 'commander_killed');
    log.info({ event: 'node_killed', node_id: nodeId });
  }

  async _distributeCampaign(campaign) {
    const queueUrl = this.nodeQueues[campaign.node_type];
    if (!queueUrl) return;
    await this.sqs.send(new SendMessageCommand({
      QueueUrl:    queueUrl,
      MessageBody: JSON.stringify({ command: 'run_campaign', campaign }),
    }));
  }

  async _sendBudgetUpdate(nodeType, newBudget) {
    const queueUrl = this.nodeQueues[nodeType];
    if (!queueUrl) return;
    await this.sqs.send(new SendMessageCommand({
      QueueUrl:    queueUrl,
      MessageBody: JSON.stringify({
        command:           'update_config',
        key:               'daily_budget_usd',
        value:             newBudget,
        broadcast_to_all:  true,
      }),
    }));
  }

  // ------------------------------------------------------------------ //
  //  CAMPAIGN MONITOR LOOP                                               //
  // ------------------------------------------------------------------ //

  async _campaignMonitorLoop() {
    while (this._running) {
      await sleep(3_600_000); // every hour
      try {
        const campaigns = await this.memory.getActiveCampaigns();
        for (const c of campaigns) {
          if (!c.created_at) continue;
          const ageH = (Date.now() - new Date(c.created_at).getTime()) / 3_600_000;
          if (ageH > 48 && (c.conversions ?? 0) === 0) {
            await this.memory.pauseCampaign(c.campaign_id);
            log.info({ event: 'campaign_auto_paused', campaign_id: c.campaign_id, age_h: Math.round(ageH) });
          }
        }
      } catch (err) {
        log.error({ event: 'campaign_monitor_error', error: err.message });
      }
    }
  }

  // ------------------------------------------------------------------ //
  //  ANALYTICS HELPERS                                                   //
  // ------------------------------------------------------------------ //

  _aggregateMetrics(nodes) {
    const totals = {};
    for (const node of nodes) {
      for (const [k, v] of Object.entries(node.metrics_summary ?? {})) {
        if (typeof v === 'number') totals[k] = (totals[k] ?? 0) + v;
      }
    }
    return totals;
  }

  _nodeTypeSummary(nodes) {
    return nodes.reduce((acc, n) => {
      const t = n.node_type ?? 'unknown';
      acc[t] = (acc[t] ?? 0) + 1;
      return acc;
    }, {});
  }

  _findUnderperformers(nodes, threshold = 0.10) {
    return nodes
      .filter(n => {
        if (n.status !== 'running') return false;
        const rates = Object.entries(n.metrics_summary ?? {})
          .filter(([k, v]) => /rate/i.test(k) && typeof v === 'number')
          .map(([, v]) => v);
        return rates.length > 0 && rates.reduce((a, b) => a + b, 0) / rates.length < threshold;
      })
      .map(n => n.node_id);
  }

  _findTopPerformers(nodes, threshold = 0.60) {
    return nodes
      .filter(n => {
        if (n.status !== 'running') return false;
        const rates = Object.entries(n.metrics_summary ?? {})
          .filter(([k, v]) => /rate/i.test(k) && typeof v === 'number')
          .map(([, v]) => v);
        return rates.length > 0 && rates.reduce((a, b) => a + b, 0) / rates.length >= threshold;
      })
      .map(n => n.node_id);
  }

  // ------------------------------------------------------------------ //
  //  BOOTSTRAP                                                           //
  // ------------------------------------------------------------------ //

  async bootstrapInitialCampaigns(goals) {
    for (const goal of goals) {
      const campaign = {
        name:              goal.name ?? 'Initial Campaign',
        node_type:         goal.primary_channel ?? 'email_node',
        goal,
        daily_budget_usd:  goal.budget ?? 10,
        target_audience:   goal.audience ?? {},
      };
      const cid = await this.memory.createCampaign(campaign);
      log.info({ event: 'bootstrap_campaign_created', campaign_id: cid, name: campaign.name });
      await this._spawnNode(campaign.node_type, campaign);
    }
  }
}
