/**
 * BaseNode — Abstract base class for every Swarm worker node.
 *
 * Every node (Email, SEO, DM, Voice, Content, Scraper, Analytics) extends this.
 * Handles:
 *   - Unique identity (node_id, generation)
 *   - Heartbeat / health reporting to SharedMemory
 *   - Metric helpers (increment, gauge, getCounter)
 *   - 24-hour self-improvement cycle
 *   - Swarm broadcasting when performance threshold is crossed
 *   - SQS task queue integration
 *   - Graceful shutdown via SIGINT/SIGTERM
 */

import { SQSClient, ReceiveMessageCommand, DeleteMessageCommand, SendMessageCommand } from '@aws-sdk/client-sqs';
import { randomUUID } from 'crypto';
import { SharedMemory } from './shared_memory.mjs';
import { SelfImprovementEngine } from './self_improvement.mjs';
import { SwarmIntelligence } from './swarm_intelligence.mjs';
import { log } from './logger.mjs';

const SWARM_SHARE_THRESHOLD = 0.80; // broadcast when score >= 80%

/** @param {number} ms */
const sleep = ms => new Promise(r => setTimeout(r, ms));

export class BaseNode {
  // Subclasses must set these as static properties
  static nodeType = 'base';

  get nodeType() { return this.constructor.nodeType; }

  // Intervals (milliseconds)
  static HEARTBEAT_INTERVAL  = 60_000;
  static IMPROVEMENT_INTERVAL = 86_400_000; // 24 hours
  static SWARM_SHARE_INTERVAL = 3_600_000;  // 1 hour

  constructor(config, region = 'us-east-1', parentId = null) {
    this.config    = config;
    this.region    = region;
    this.nodeId    = config.node_id ?? `${this.nodeType}-${randomUUID().slice(0, 8)}`;
    this.generation = config.generation ?? 1;
    this.parentId  = parentId;
    this.startedAt = new Date();
    this._running  = false;
    this._counters = {};

    this.memory   = new SharedMemory(region);
    this.improver = new SelfImprovementEngine({ memory: this.memory, nodeType: this.nodeType });
    this.swarmIQ  = new SwarmIntelligence({ memory: this.memory, nodeType: this.nodeType });
    this.sqs      = new SQSClient({ region });
    this.queueUrl = config.queue_url ?? '';

    // Self-destruct settings
    // Seed nodes (id contains 'seed') never self-destruct by default
    const isSeed              = this.nodeId.includes('seed');
    this.allowSelfDestruct    = config.allow_self_destruct    ?? !isSeed;
    this.maxFailedCycles      = config.max_failed_cycles      ?? Number(process.env.SWARM_MAX_FAILED_CYCLES   ?? 3);
    this.selfDestructThreshold = config.self_destruct_threshold ?? Number(process.env.SWARM_KILLER_THRESHOLD ?? 0.01);
    this._failedCycles        = 0;
    this._improvementHistory  = []; // [{cycle, configDiff, scoreBefore, scoreAfter}]

    log.info({ event: 'node_created', node_id: this.nodeId, node_type: this.nodeType, generation: this.generation });
  }

  // ------------------------------------------------------------------ //
  //  ABSTRACT — subclasses MUST override these                          //
  // ------------------------------------------------------------------ //

  /** @returns {Promise<void>} */
  async runCycle() { throw new Error(`${this.nodeType}.runCycle() not implemented`); }

  /** @returns {Record<string, number>} */
  collectMetrics() { throw new Error(`${this.nodeType}.collectMetrics() not implemented`); }

  /** @returns {Record<string, any>} */
  getImprovementContext() { throw new Error(`${this.nodeType}.getImprovementContext() not implemented`); }

  // ------------------------------------------------------------------ //
  //  LIFECYCLE                                                           //
  // ------------------------------------------------------------------ //

  async start() {
    this._running = true;

    await this.memory.registerNode(this.nodeId, this.nodeType, {
      ...this.config,
      generation: this.generation,
      parent_id:  this.parentId,
    });

    await this._absorbSwarmKnowledge();

    // Graceful shutdown
    const shutdown = async (sig) => {
      log.info({ event: 'signal_received', signal: sig, node_id: this.nodeId });
      await this.stop('signal');
      process.exit(0);
    };
    process.once('SIGINT',  () => shutdown('SIGINT'));
    process.once('SIGTERM', () => shutdown('SIGTERM'));

    log.info({ event: 'node_starting', node_id: this.nodeId });

    // Run all loops concurrently — any one resolving ends the node
    await Promise.allSettled([
      this._workLoop(),
      this._heartbeatLoop(),
      this._improvementLoop(),
      this._swarmShareLoop(),
    ]);
  }

  async stop(reason = 'shutdown') {
    log.info({ event: 'node_stopping', node_id: this.nodeId, reason });
    this._running = false;
    await this.memory.deregisterNode(this.nodeId, reason);
  }

  // ------------------------------------------------------------------ //
  //  LOOPS                                                               //
  // ------------------------------------------------------------------ //

  async _workLoop() {
    const cycleSleep = (this.config.cycle_sleep ?? 10) * 1000;
    while (this._running) {
      try {
        await this.runCycle();
      } catch (err) {
        log.error({ event: 'cycle_error', node_id: this.nodeId, error: err.message });
        this.increment('errors');
      }
      await sleep(cycleSleep);
    }
  }

  async _heartbeatLoop() {
    while (this._running) {
      await sleep(this.constructor.HEARTBEAT_INTERVAL);
      try {
        const summary = this.collectMetrics();
        await this.memory.heartbeat(this.nodeId, summary);
        for (const [name, val] of Object.entries(summary)) {
          await this.memory.writeMetric(this.nodeId, this.nodeType, name, val);
        }
      } catch (err) {
        log.warn({ event: 'heartbeat_failed', node_id: this.nodeId, error: err.message });
      }
    }
  }

  async _improvementLoop() {
    let cycleNumber = 0;
    while (this._running) {
      await sleep(this.constructor.IMPROVEMENT_INTERVAL);
      try {
        cycleNumber++;
        log.info({ event: 'self_improvement_start', node_id: this.nodeId, cycle: cycleNumber });

        const ctx         = this.getImprovementContext();
        const scoreBefore = this._computeScore(this.collectMetrics());
        const update      = await this.improver.run(this.nodeId, ctx);

        if (update && Object.keys(update).length) {
          this._applyImprovement(update);
          log.info({ event: 'self_improvement_applied', node_id: this.nodeId, cycle: cycleNumber, keys: Object.keys(update) });
        }

        const scoreAfter = this._computeScore(this.collectMetrics());

        // Track history for failure report
        this._improvementHistory.push({
          cycle:       cycleNumber,
          configDiff:  update ?? {},
          scoreBefore,
          scoreAfter,
          timestamp:   new Date().toISOString(),
        });

        // Self-destruct eligibility check
        if (this.allowSelfDestruct) {
          if (scoreAfter < this.selfDestructThreshold) {
            this._failedCycles++;
            log.warn({
              event:         'failed_improvement_cycle',
              node_id:        this.nodeId,
              cycle:          cycleNumber,
              score:          scoreAfter,
              failed_cycles:  this._failedCycles,
              max_before_destruct: this.maxFailedCycles,
            });
            if (this._failedCycles >= this.maxFailedCycles) {
              await this._selfDestruct(ctx, this.collectMetrics(), scoreAfter);
              return; // exit loop — process will exit inside _selfDestruct
            }
          } else {
            // Any improvement resets the failure counter
            if (this._failedCycles > 0) {
              log.info({ event: 'failure_counter_reset', node_id: this.nodeId, score: scoreAfter });
              this._failedCycles = 0;
            }
          }
        }
      } catch (err) {
        log.error({ event: 'improvement_error', node_id: this.nodeId, error: err.message });
      }
    }
  }

  async _swarmShareLoop() {
    while (this._running) {
      await sleep(this.constructor.SWARM_SHARE_INTERVAL);
      try {
        const metrics = this.collectMetrics();
        const score   = this._computeScore(metrics);
        if (score >= SWARM_SHARE_THRESHOLD) {
          const ctx = this.getImprovementContext();
          await this.swarmIQ.broadcastSuccess(this.nodeId, score, ctx, metrics);
          log.info({ event: 'swarm_knowledge_shared', node_id: this.nodeId, score });
        }
      } catch (err) {
        log.warn({ event: 'swarm_share_error', node_id: this.nodeId, error: err.message });
      }
    }
  }

  async _absorbSwarmKnowledge() {
    try {
      const [patterns, failures] = await Promise.all([
        this.swarmIQ.getBestPatterns(5),
        this.memory.getFailureKnowledge(this.nodeType, 10),
      ]);
      if (patterns.length) {
        log.info({ event: 'absorbing_swarm_knowledge', node_id: this.nodeId, count: patterns.length });
        this._integratePatterns(patterns);
      }
      if (failures.length) {
        log.info({ event: 'absorbing_failure_knowledge', node_id: this.nodeId, failures: failures.length });
        this._integrateFailureKnowledge(failures);
      }
    } catch (err) {
      log.warn({ event: 'absorb_knowledge_error', node_id: this.nodeId, error: err.message });
    }
  }

  _integratePatterns(patterns) {
    for (const p of patterns) {
      const key = p.data?.config_key;
      const val = p.data?.config_value;
      if (key && val !== undefined && !(key in this.config)) {
        this.config[key] = val;
      }
    }
  }

  /**
   * Absorb negative knowledge from dead nodes.
   * Any config value that is flagged in a failure report gets cleared
   * so the improvement engine finds a different value instead.
   */
  _integrateFailureKnowledge(failures) {
    for (const f of failures) {
      const avoid = f.data?.configs_to_avoid ?? {};
      const bad   = f.data?.segments_or_targets_to_avoid ?? [];
      for (const [key, val] of Object.entries(avoid)) {
        // If we currently have the exact same config value that killed a predecessor, clear it
        if (JSON.stringify(this.config[key]) === JSON.stringify(val)) {
          log.debug({ event: 'avoiding_failed_config', node_id: this.nodeId, key, val });
          delete this.config[key];
        }
      }
      // Store bad targets in config so the node's own logic can filter them out
      if (bad.length) {
        this.config._known_bad_targets = [
          ...(this.config._known_bad_targets ?? []),
          ...bad,
        ].slice(-50); // cap at 50 entries
      }
    }
  }

  // ------------------------------------------------------------------ //
  //  SELF-DESTRUCT                                                       //
  // ------------------------------------------------------------------ //

  /**
   * Called after maxFailedCycles consecutive improvement cycles below threshold.
   * 1. Generates LLM failure report
   * 2. Broadcasts failure knowledge to the swarm
   * 3. Deregisters from DynamoDB
   * 4. Exits the process
   */
  async _selfDestruct(ctx, metrics, finalScore) {
    log.warn({
      event:         'self_destruct_initiated',
      node_id:       this.nodeId,
      node_type:     this.nodeType,
      generation:    this.generation,
      final_score:   finalScore,
      failed_cycles: this._failedCycles,
      uptime_hours:  +((Date.now() - this.startedAt.getTime()) / 3_600_000).toFixed(2),
    });

    try {
      // 1. Generate structured failure report via LLM
      const report = await this.improver.generateFailureReport(
        this.nodeId,
        { ...ctx, generation: this.generation, parent_id: this.parentId },
        metrics,
        this._failedCycles,
        this._improvementHistory,
      );

      log.warn({ event: 'failure_report_generated', node_id: this.nodeId, summary: report.what_was_tried });

      // 2. Broadcast failure knowledge so successors avoid the same path
      await this.memory.storeKnowledge(
        this.nodeType,
        'failure',
        {
          ...report,
          node_id:          this.nodeId,
          generation:       this.generation,
          parent_id:        this.parentId,
          config_at_death:  { ...this.config },
          metrics_at_death: metrics,
          final_score:      finalScore,
          failed_cycles:    this._failedCycles,
          improvement_history: this._improvementHistory,
        },
        0, // score 0 — marks it as negative knowledge
      );

      log.warn({ event: 'failure_knowledge_broadcast', node_id: this.nodeId, node_type: this.nodeType });
    } catch (err) {
      log.error({ event: 'self_destruct_report_failed', node_id: this.nodeId, error: err.message });
    }

    // 3. Graceful shutdown
    await this.stop('self_destruct_underperformer');
    process.exit(0);
  }

  _applyImprovement(update) {
    for (const [key, value] of Object.entries(update)) {
      this.config[key] = value;
      log.debug({ event: 'config_updated', node_id: this.nodeId, key, value });
    }
  }

  _computeScore(metrics) {
    const rates = Object.entries(metrics)
      .filter(([k, v]) => /rate/i.test(k) && v >= 0 && v <= 1)
      .map(([, v]) => v);
    return rates.length ? rates.reduce((a, b) => a + b, 0) / rates.length : 0;
  }

  // ------------------------------------------------------------------ //
  //  METRIC HELPERS                                                      //
  // ------------------------------------------------------------------ //

  increment(counter, amount = 1) {
    this._counters[counter] = (this._counters[counter] ?? 0) + amount;
  }

  gauge(key, value) {
    this._counters[key] = value;
  }

  getCounter(key, defaultVal = 0) {
    return this._counters[key] ?? defaultVal;
  }

  // ------------------------------------------------------------------ //
  //  SQS HELPERS                                                         //
  // ------------------------------------------------------------------ //

  async receiveTasks(maxMessages = 10) {
    if (!this.queueUrl) return [];
    try {
      const resp = await this.sqs.send(new ReceiveMessageCommand({
        QueueUrl:            this.queueUrl,
        MaxNumberOfMessages: maxMessages,
        WaitTimeSeconds:     5,
      }));
      return (resp.Messages ?? []).map(msg => {
        try {
          const body = JSON.parse(msg.Body);
          body._receipt_handle = msg.ReceiptHandle;
          return body;
        } catch {
          return null;
        }
      }).filter(Boolean);
    } catch (err) {
      log.warn({ event: 'sqs_receive_error', node_id: this.nodeId, error: err.message });
      return [];
    }
  }

  async ackTask(receiptHandle) {
    if (!this.queueUrl) return;
    await this.sqs.send(new DeleteMessageCommand({
      QueueUrl:      this.queueUrl,
      ReceiptHandle: receiptHandle,
    }));
  }

  async sendResult(resultQueueUrl, result) {
    await this.sqs.send(new SendMessageCommand({
      QueueUrl:    resultQueueUrl,
      MessageBody: JSON.stringify({ ...result, node_id: this.nodeId, node_type: this.nodeType }),
    }));
  }
}
