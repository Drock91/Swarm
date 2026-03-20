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
import Docker from 'dockerode';
import OpenAI from 'openai';
import { SharedMemory } from './shared_memory.mjs';
import { SwarmIntelligence } from './swarm_intelligence.mjs';
import { log } from './logger.mjs';

// Docker socket — works inside a container on any OS
const DOCKER_OPTS = process.platform === 'win32' && !process.env.RUNNING_IN_DOCKER
  ? { socketPath: '//./pipe/docker_engine' }
  : { socketPath: '/var/run/docker.sock' };

const sleep = ms => new Promise(r => setTimeout(r, ms));

const COMMANDER_SYSTEM_PROMPT = `
You are the Commander Agent of an autonomous marketing swarm.
You orchestrate multiple specialized nodes (email, SEO, scraper, analytics).

Given current metrics, active campaigns, and node statuses, decide:
1. Which campaigns to prioritize or pause
2. Which nodes to scale up (spawn another instance / clone) or kill
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
    this.nodeQueues      = config.node_queues ?? {};        // { email_node: 'https://sqs...' }
    this.taskDefinitions = config.task_definitions ?? {};   // { email_node: 'swarm-email:3' }
    this.ecsCluster      = config.ecs_cluster ?? 'swarm-cluster';

    // Runtime mode: 'local' (Docker) or 'fargate' (ECS)
    this.runtime         = (config.runtime ?? process.env.RUNTIME ?? 'local').toLowerCase();
    this.docker          = this.runtime === 'local' ? new Docker(DOCKER_OPTS) : null;
    this.dockerImage     = config.docker_image ?? process.env.SWARM_DOCKER_IMAGE   ?? 'the-swarm-node';
    this.dockerNetwork    = config.docker_network ?? process.env.SWARM_DOCKER_NETWORK ?? 'the-swarm_swarm-net';

    // Clone management
    this.maxEmailClones   = config.max_email_clones   ?? Number(process.env.SWARM_MAX_EMAIL_CLONES ?? 5);
    this.cloneThreshold   = config.clone_threshold    ?? Number(process.env.SWARM_CLONE_THRESHOLD  ?? 0.05);
    this.killerThreshold  = config.killer_threshold   ?? Number(process.env.SWARM_KILLER_THRESHOLD ?? 0.01);
    this.cloneWindowHours = config.clone_window_hours ?? Number(process.env.SWARM_CLONE_WINDOW_H   ?? 48);

    // Sender identities for clones: comma-separated emails in SWARM_SENDER_EMAILS
    this.senderEmails = (process.env.SWARM_SENDER_EMAILS ?? '')
      .split(',')
      .map(s => s.trim())
      .filter(Boolean);

    log.info({ event: 'commander_created', commander_id: this.nodeId, runtime: this.runtime });
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
    log.info({ event: 'commander_starting', commander_id: this.nodeId, runtime: this.runtime });

    process.once('SIGINT',  async () => { await this.stop(); process.exit(0); });
    process.once('SIGTERM', async () => { await this.stop(); process.exit(0); });

    await Promise.allSettled([
      this._orchestrationLoop(),
      this._campaignMonitorLoop(),
      this._cloneMonitorLoop(),
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

  async _spawnNode(nodeType, baseConfig = {}, senderEmail = null) {
    const swarmIQ     = this._swarmFor(nodeType);
    const synthConfig = await swarmIQ.synthesizeOptimalConfig(5) ?? {};
    const merged      = { ...synthConfig, ...baseConfig };

    if (this.runtime === 'local') {
      return this._spawnDockerNode(nodeType, merged, senderEmail);
    }
    return this._spawnFargateNode(nodeType, merged);
  }

  // ── Local Docker spawn ──────────────────────────────────────────── //

  async _spawnDockerNode(nodeType, config = {}, senderEmail = null) {
    const nodeId = `${nodeType.replace('_node', '')}-clone-${randomUUID().slice(0, 6)}`;
    const env = [
      `NODE_TYPE=${nodeType}`,
      `NODE_ID=${nodeId}`,
      `AWS_REGION=${this.region}`,
      `AWS_ACCESS_KEY_ID=${process.env.AWS_ACCESS_KEY_ID ?? ''}`,
      `AWS_SECRET_ACCESS_KEY=${process.env.AWS_SECRET_ACCESS_KEY ?? ''}`,
      `OPENAI_API_KEY=${process.env.OPENAI_API_KEY ?? ''}`,
      `SENDGRID_API_KEY=${process.env.SENDGRID_API_KEY ?? ''}`,
      `SENDGRID_FROM_NAME=${process.env.SENDGRID_FROM_NAME ?? ''}`,
      `SENDGRID_FROM_EMAIL=${senderEmail ?? process.env.SENDGRID_FROM_EMAIL ?? ''}`,
      `NODE_CONFIG=${JSON.stringify(config)}`,
      `RUNNING_IN_DOCKER=1`,
      // Pass all SWARM_* queue URLs through
      ...Object.entries(process.env)
        .filter(([k]) => k.startsWith('SWARM_') && k.endsWith('_QUEUE_URL'))
        .map(([k, v]) => `${k}=${v}`),
    ];

    try {
      const container = await this.docker.createContainer({
        Image:      this.dockerImage,
        name:       nodeId,
        Env:        env,
        HostConfig: { NetworkMode: this.dockerNetwork, AutoRemove: false, RestartPolicy: { Name: 'unless-stopped' } },
      });
      await container.start();
      log.info({ event: 'docker_node_spawned', node_id: nodeId, node_type: nodeType, container_id: container.id.slice(0, 12) });
      // Register in DynamoDB so status dashboard picks it up
      await this.memory.registerNode(nodeId, nodeType, { container_id: container.id, generation: config.generation ?? 1, sender_email: senderEmail });
      return container.id;
    } catch (err) {
      log.error({ event: 'docker_spawn_error', node_type: nodeType, error: err.message });
      return null;
    }
  }

  // ── Fargate spawn ───────────────────────────────────────────────── //

  async _spawnFargateNode(nodeType, config = {}) {
    const taskDef = this.taskDefinitions[nodeType];
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
              { name: 'NODE_CONFIG', value: JSON.stringify(config) },
              { name: 'NODE_TYPE',   value: nodeType },
            ],
          }],
        },
      }));
      const taskArn = resp.tasks?.[0]?.taskArn ?? null;
      log.info({ event: 'fargate_node_spawned', node_type: nodeType, task_arn: taskArn });
      return taskArn;
    } catch (err) {
      log.error({ event: 'fargate_spawn_error', node_type: nodeType, error: err.message });
      return null;
    }
  }

  async _killNode(nodeId) {
    const node = await this.memory.getNode(nodeId);
    if (!node) return;

    // Send graceful shutdown via SQS first
    const queueUrl = this.nodeQueues[node.node_type];
    if (queueUrl) {
      await this.sqs.send(new SendMessageCommand({
        QueueUrl:    queueUrl,
        MessageBody: JSON.stringify({ command: 'shutdown', target_node_id: nodeId }),
      }));
    }

    // If running locally, also stop + remove the Docker container
    if (this.runtime === 'local' && node.container_id) {
      try {
        const c = this.docker.getContainer(node.container_id);
        await c.stop({ t: 10 }).catch(() => {});
        await c.remove().catch(() => {});
        log.info({ event: 'docker_container_removed', node_id: nodeId });
      } catch (err) {
        log.warn({ event: 'docker_remove_warn', node_id: nodeId, error: err.message });
      }
    }

    await this.memory.deregisterNode(nodeId, 'commander_killed');
    log.info({ event: 'node_killed', node_id: nodeId });
  }

  // ------------------------------------------------------------------ //
  //  CLONE MONITOR LOOP  (reply-rate threshold + genetic evolution)      //
  // ------------------------------------------------------------------ //

  async _cloneMonitorLoop() {
    const checkInterval = 30 * 60 * 1000; // every 30 minutes
    while (this._running) {
      await sleep(checkInterval);
      try {
        await this._evaluateEmailClones();
      } catch (err) {
        log.error({ event: 'clone_monitor_error', error: err.message });
      }
    }
  }

  async _evaluateEmailClones() {
    const nodes        = await this.memory.getAllNodes();
    const emailNodes   = nodes.filter(n => n.node_type === 'email_node' && n.status === 'running');
    const clones       = emailNodes.filter(n => n.node_id.includes('clone'));
    const windowMs     = this.cloneWindowHours * 3_600_000;

    if (!emailNodes.length) return;

    for (const node of emailNodes) {
      const m       = node.metrics_summary ?? {};
      const replyRate = m.reply_rate ?? 0;
      const ageMs   = node.started_at ? Date.now() - new Date(node.started_at).getTime() : 0;

      // Kill consistent underperformers after window has elapsed
      if (ageMs > windowMs && replyRate < this.killerThreshold && node.node_id.includes('clone')) {
        log.info({ event: 'killing_underperformer', node_id: node.node_id, reply_rate: replyRate });
        await this._killNode(node.node_id);
        continue;
      }

      // Spawn a child clone from a winner
      if (replyRate >= this.cloneThreshold && clones.length < this.maxEmailClones) {
        log.info({ event: 'winner_detected', node_id: node.node_id, reply_rate: replyRate, spawning_child: true });
        const parentConfig  = node.node_config ?? {};
        const newGeneration = (parentConfig.generation ?? 1) + 1;
        const swarmIQ       = this._swarmFor('email_node');
        const evolvedConfig = await swarmIQ.spawnImprovedNodeConfig(parentConfig, replyRate) ?? {};
        evolvedConfig.generation      = newGeneration;
        evolvedConfig.parent_node_id  = node.node_id;

        // Pick a sender email not already in use
        const usedEmails   = emailNodes.map(n => n.sender_email).filter(Boolean);
        const availEmail   = this.senderEmails.find(e => !usedEmails.includes(e)) ?? null;

        await this._spawnNode('email_node', evolvedConfig, availEmail);
      }
    }

    const activeClones = (await this.memory.getAllNodes())
      .filter(n => n.node_type === 'email_node' && n.node_id.includes('clone') && n.status === 'running');
    log.info({ event: 'clone_monitor_tick', email_nodes: emailNodes.length, active_clones: activeClones.length, max: this.maxEmailClones });
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
