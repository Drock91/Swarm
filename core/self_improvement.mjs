/**
 * SelfImprovementEngine — Per-node 24-hour optimization loop.
 *
 * Each node runs its own improvement cycle independently:
 * 1. Pull last 24h metrics from SharedMemory
 * 2. Identify winning vs losing copy / config variants
 * 3. Ask LLM to suggest improvements
 * 4. Return config diff to apply on the live node
 * 5. Losing variants get killed; winners get cloned with variation
 */

import OpenAI from 'openai';
import { log } from './logger.mjs';

const IMPROVEMENT_SYSTEM_PROMPT = `
You are an expert digital marketing optimization AI embedded inside an autonomous agent node.
You receive performance metrics and campaign context for a single marketing node.
Your job is to analyze what is working, what is failing, and output concrete config changes
that will improve performance over the next 24 hours.

You must respond ONLY with a valid JSON object where keys are config field names and values
are the new settings to apply. If no changes are needed, return an empty object {}.

Focus areas depending on node type:
- email_node:   subject lines, send times, follow-up intervals, list segments
- seo_node:     keyword targets, content angle, link-build strategy
- dm_node:      opening message, follow-up cadence, target persona
- voice_node:   call script, call times, persona tone
- content_node: post formats, hooks, platforms, posting frequency
- scraper_node: data sources, scrape frequency, targeting filters
- analytics_node: report cadence, key metrics to surface

Be specific, data-driven, and output only the JSON.
`.trim();

export class SelfImprovementEngine {
  /**
   * @param {{ memory: import('./shared_memory.mjs').SharedMemory }} opts
   */
  constructor({ memory, nodeType, llmModel = 'gpt-4o' }) {
    this.memory    = memory;
    this.nodeType  = nodeType;
    this.llmModel  = llmModel;
    this.openai    = new OpenAI();
  }

  // ------------------------------------------------------------------ //
  //  MAIN IMPROVEMENT CYCLE                                              //
  // ------------------------------------------------------------------ //

  /**
   * Run the 24-hour improvement cycle.
   * @returns {Promise<object|null>} config diff to apply, or null
   */
  async run(nodeId, context) {
    const metrics24h = await this._loadRecentMetrics(nodeId);
    const campaigns  = await this.memory.getActiveCampaigns(this.nodeType);
    const knowledge  = await this.memory.getTopKnowledge(this.nodeType, 'config', 5);

    const prompt = this._buildPrompt({ nodeId, context, metrics24h, campaigns, knowledge });

    try {
      const resp = await this.openai.chat.completions.create({
        model:           this.llmModel,
        messages:        [
          { role: 'system', content: IMPROVEMENT_SYSTEM_PROMPT },
          { role: 'user',   content: prompt },
        ],
        response_format: { type: 'json_object' },
        temperature:     0.3,
      });

      const update = JSON.parse(resp.choices[0].message.content);

      if (update && Object.keys(update).length > 0) {
        log.info({
          event:    'improvement_plan_generated',
          node_id:  nodeId,
          changes:  Object.keys(update).length,
          model:    this.llmModel,
        });
        await this.memory.storeKnowledge(
          this.nodeType,
          'config',
          { config_update: update, rationale: context },
          this._scoreFromMetrics(metrics24h),
        );
        return update;
      }
      return null;
    } catch (err) {
      log.error({ event: 'improvement_llm_error', node_id: nodeId, error: err.message });
      return null;
    }
  }

  // ------------------------------------------------------------------ //
  //  COPY EVOLUTION                                                      //
  // ------------------------------------------------------------------ //

  /**
   * Clone winning copy with variation and rewrite losing copy.
   * @returns {Promise<string[]>} new copy variants to A/B test
   */
  async evolveCopy(nodeId, winningCopy, losingCopy, context) {
    const count  = Math.max(winningCopy.length + losingCopy.length, 3);
    const prompt = `
You are a world-class direct-response copywriter.
Node type: ${this.nodeType}
Context: ${JSON.stringify(context, null, 2)}

WINNING copy (high conversion — clone and vary these):
${JSON.stringify(winningCopy, null, 2)}

LOSING copy (low conversion — rewrite from scratch):
${JSON.stringify(losingCopy, null, 2)}

Generate ${count} new copy variants.
For winners: keep the core hook, vary the angle, CTA, or framing.
For losers: completely new approach based on a different psychological trigger.
Respond with JSON: { "variants": ["...", "..."] }
`.trim();

    try {
      const resp = await this.openai.chat.completions.create({
        model:           this.llmModel,
        messages:        [{ role: 'user', content: prompt }],
        response_format: { type: 'json_object' },
        temperature:     0.7,
      });
      const result = JSON.parse(resp.choices[0].message.content);
      if (Array.isArray(result)) return result;
      return result.variants ?? result.copy ?? Object.values(result)[0] ?? [];
    } catch (err) {
      log.error({ event: 'copy_evolution_error', node_id: nodeId, error: err.message });
      return [];
    }
  }

  // ------------------------------------------------------------------ //
  //  INTERNALS                                                           //
  // ------------------------------------------------------------------ //

  async _loadRecentMetrics(nodeId) {
    const since = Math.floor(Date.now() / 1000) - 86400;
    const names = [
      'leads_found', 'emails_sent', 'open_rate', 'click_rate',
      'reply_rate', 'conversion_rate', 'calls_made', 'calls_connected',
      'human_replies', 'content_published', 'backlinks_earned',
      'dms_sent', 'errors', 'cost_usd',
    ];
    const result = {};
    for (const name of names) {
      const rows = await this.memory.getMetrics(nodeId, name, since);
      if (rows.length) result[name] = rows.map(r => r.value);
    }
    return result;
  }

  _buildPrompt({ nodeId, context, metrics24h, campaigns, knowledge }) {
    const summarize = (vals) => {
      if (!vals?.length) return {};
      return {
        count: vals.length,
        total: +vals.reduce((a, b) => a + b, 0).toFixed(4),
        avg:   +(vals.reduce((a, b) => a + b, 0) / vals.length).toFixed(4),
        max:   +Math.max(...vals).toFixed(4),
        min:   +Math.min(...vals).toFixed(4),
      };
    };
    return JSON.stringify({
      node_id:          nodeId,
      node_type:        this.nodeType,
      current_config:   context.config ?? {},
      metrics_24h:      Object.fromEntries(Object.entries(metrics24h).map(([k, v]) => [k, summarize(v)])),
      active_campaigns: campaigns.slice(0, 5).map(c => ({ id: c.campaign_id, name: c.name })),
      top_knowledge:    knowledge.slice(0, 3).map(k => ({ score: k.score, data: k.data })),
    }, null, 2);
  }

  _scoreFromMetrics(metrics24h) {
    const rateKeys = ['open_rate', 'click_rate', 'reply_rate', 'conversion_rate'];
    const scores   = [];
    for (const k of rateKeys) {
      const vals = metrics24h[k];
      if (vals?.length) scores.push(vals.reduce((a, b) => a + b, 0) / vals.length);
    }
    return scores.length ? +(scores.reduce((a, b) => a + b, 0) / scores.length).toFixed(4) : 0;
  }

  // ------------------------------------------------------------------ //
  //  FAILURE REPORT (self-destruct knowledge broadcast)                  //
  // ------------------------------------------------------------------ //

  /**
   * Called when a node is about to self-destruct after maxFailedCycles.
   * Generates a structured LLM report of what was tried and why it failed
   * so successor nodes can avoid repeating the same mistakes.
   *
   * @param {string} nodeId
   * @param {object} context      - node's getImprovementContext() output
   * @param {object} metrics      - collectMetrics() output at time of death
   * @param {number} failedCycles - how many improvement cycles failed
   * @param {Array}  history      - improvement history [{cycle, configDiff, score}]
   * @returns {Promise<object>}
   */
  async generateFailureReport(nodeId, context, metrics, failedCycles, history = []) {
    const prompt = JSON.stringify({
      node_id:          nodeId,
      node_type:        this.nodeType,
      generation:       context.generation ?? 1,
      parent_id:        context.parent_id ?? null,
      failed_cycles:    failedCycles,
      final_metrics:    metrics,
      current_config:   context.config ?? {},
      improvement_history: history.slice(-failedCycles).map(h => ({
        cycle:          h.cycle,
        config_changes: h.configDiff,
        score_before:   h.scoreBefore,
        score_after:    h.scoreAfter,
      })),
    }, null, 2);

    try {
      const resp = await this.openai.chat.completions.create({
        model:           this.llmModel,
        messages: [
          {
            role:    'system',
            content: `You analyze why an autonomous ${this.nodeType} marketing agent failed to reach its
performance threshold after ${failedCycles} optimization cycles and is shutting itself down.

Your analysis will be stored in the swarm knowledge base and read by future nodes
before they start, so they don't repeat these mistakes.

Respond ONLY with a JSON object:
{
  "what_was_tried":                  "plain-english summary of configs attempted",
  "likely_failure_reasons":          ["reason 1", "reason 2", ...],
  "configs_to_avoid":                { "config_key": value, ... },
  "segments_or_targets_to_avoid":    ["..."],
  "recommendations_for_successors":  "specific actionable advice for the next generation"
}`.trim(),
          },
          { role: 'user', content: prompt },
        ],
        response_format: { type: 'json_object' },
        temperature:     0.3,
      });
      return JSON.parse(resp.choices[0].message.content);
    } catch (err) {
      log.error({ event: 'failure_report_llm_error', node_id: nodeId, error: err.message });
      return {
        what_was_tried:                 'unknown — LLM unavailable at time of death',
        likely_failure_reasons:        ['LLM call failed'],
        configs_to_avoid:              context.config ?? {},
        segments_or_targets_to_avoid:  [],
        recommendations_for_successors: 'Review metrics manually',
      };
    }
  }
}
