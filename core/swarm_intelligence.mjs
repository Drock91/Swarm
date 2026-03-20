/**
 * SwarmIntelligence — Cross-node knowledge sharing layer.
 *
 * When any individual node crosses the performance threshold, it broadcasts
 * its winning patterns to the swarm. Other nodes of the same type absorb
 * those patterns on their next startup or improvement cycle.
 *
 * Emergent collective learning:
 *   Individual node learns → shares → all nodes of that type improve
 *   → next generation starts ahead of where this one started
 */

import OpenAI from 'openai';
import { log } from './logger.mjs';

const SYNTHESIS_SYSTEM_PROMPT = `
You are a swarm intelligence synthesis engine. You receive multiple high-performing
patterns from successful marketing agent nodes of the same type, and your job is to
synthesize them into a generalized best-practice config that future nodes can start with.

Extract the common principles, the best-performing settings, and the key insights.
Respond with a JSON object that represents generalized optimal starting config for
a new node of this type.
`.trim();

export class SwarmIntelligence {
  constructor({ memory, nodeType, llmModel = 'gpt-4o' }) {
    this.memory   = memory;
    this.nodeType = nodeType;
    this.llmModel = llmModel;
    this.openai   = new OpenAI();
  }

  // ------------------------------------------------------------------ //
  //  BROADCAST                                                           //
  // ------------------------------------------------------------------ //

  /**
   * A high-performing node calls this to share what made it successful.
   * @returns {Promise<string>} knowledge_id
   */
  async broadcastSuccess(nodeId, score, context, metrics) {
    const shareable = this._extractShareablePatterns(context, metrics);

    const kid = await this.memory.storeKnowledge(
      this.nodeType,
      'swarm_broadcast',
      {
        source_node:       nodeId,
        score,
        config_snapshot:   context.config ?? {},
        copy_examples:     (context.winning_copy ?? []).slice(0, 5),
        targeting_insight: (context.best_segments ?? []).slice(0, 3),
        metrics_snapshot:  metrics,
        shareable_patterns: shareable,
      },
      score,
    );

    log.info({
      event:        'swarm_broadcast_stored',
      source_node:  nodeId,
      node_type:    this.nodeType,
      score,
      knowledge_id: kid,
    });
    return kid;
  }

  // ------------------------------------------------------------------ //
  //  ABSORB PATTERNS                                                     //
  // ------------------------------------------------------------------ //

  /** Retrieve top N swarm-broadcast patterns for this node type. */
  async getBestPatterns(topN = 5) {
    return this.memory.getTopKnowledge(this.nodeType, 'swarm_broadcast', topN);
  }

  // ------------------------------------------------------------------ //
  //  SYNTHESIS (LLM)                                                     //
  // ------------------------------------------------------------------ //

  /**
   * Synthesize all top-performing broadcasts into an optimal starting config
   * for new nodes. Called by Commander when spawning a new generation.
   * @returns {Promise<object|null>}
   */
  async synthesizeOptimalConfig(topN = 10) {
    const patterns = await this.getBestPatterns(topN);
    if (!patterns.length) return null;

    const prompt = `
Node type: ${this.nodeType}

Here are ${patterns.length} top-performing patterns from successful nodes:

${JSON.stringify(patterns.map(p => p.data ?? {}), null, 2)}

Synthesize these into an optimal starting configuration for a new ${this.nodeType} node.
Focus on: messaging approach, targeting, timing, rate limits, and any proven tactics.
Return a clean JSON config object.
`.trim();

    try {
      const resp = await this.openai.chat.completions.create({
        model:           this.llmModel,
        messages:        [
          { role: 'system', content: SYNTHESIS_SYSTEM_PROMPT },
          { role: 'user',   content: prompt },
        ],
        response_format: { type: 'json_object' },
        temperature:     0.2,
      });
      const result = JSON.parse(resp.choices[0].message.content);
      log.info({ event: 'swarm_synthesis_complete', node_type: this.nodeType, fields: Object.keys(result).length });
      return result;
    } catch (err) {
      log.error({ event: 'swarm_synthesis_error', node_type: this.nodeType, error: err.message });
      return null;
    }
  }

  // ------------------------------------------------------------------ //
  //  SPAWN NEXT-GENERATION CONFIG                                        //
  // ------------------------------------------------------------------ //

  /**
   * For high performers: build config for a child node (next generation)
   * that inherits parent's best traits but tries new variations.
   */
  async spawnImprovedNodeConfig(currentConfig, performanceScore) {
    const baseConfig = await this.synthesizeOptimalConfig(5) ?? {};
    return {
      ...baseConfig,
      ...currentConfig,
      generation:    (currentConfig.generation ?? 1) + 1,
      parent_id:     currentConfig.node_id,
      initial_score: performanceScore,
    };
  }

  // ------------------------------------------------------------------ //
  //  LEADERBOARD                                                         //
  // ------------------------------------------------------------------ //

  async getSwarmLeaderboard() {
    const patterns = await this.memory.getTopKnowledge(this.nodeType, 'swarm_broadcast', 50);
    return patterns
      .map(p => ({
        node_id:    p.data?.source_node,
        score:      p.score ?? 0,
        generation: p.data?.config_snapshot?.generation ?? 1,
        metrics:    p.data?.metrics_snapshot ?? {},
      }))
      .sort((a, b) => b.score - a.score);
  }

  // ------------------------------------------------------------------ //
  //  INTERNALS                                                           //
  // ------------------------------------------------------------------ //

  _extractShareablePatterns(context, metrics) {
    const config = context.config ?? {};
    const topMetric = Object.entries(metrics).sort((a, b) => b[1] - a[1])[0] ?? ['none', 0];
    return {
      timing_config: Object.fromEntries(
        Object.entries(config).filter(([k]) => /time|interval/i.test(k)),
      ),
      rate_config: Object.fromEntries(
        Object.entries(config).filter(([k]) => /rate|limit/i.test(k)),
      ),
      target_config: Object.fromEntries(
        Object.entries(config).filter(([k]) => /target|segment/i.test(k)),
      ),
      top_metric:  topMetric,
      generation:  config.generation ?? 1,
    };
  }
}
