/**
 * AnalyticsNode — Reads all metrics, writes performance reports, feeds the brain.
 *
 * Capabilities:
 *   - Aggregates metrics from all running nodes
 *   - Identifies winning channels, copy, and targeting
 *   - Writes daily performance reports to S3
 *   - Surfaces anomalies and growth opportunities to Commander
 *   - Feeds insights into the knowledge base (SharedMemory)
 *   - Powers the 24-hour self-improvement loop with data
 *   - Optional: pushes summary to Slack or email
 */

import { chatJSON } from '../core/llm.mjs';
import { BaseNode } from '../core/base_node.mjs';
import { log } from '../core/logger.mjs';

const sleep = ms => new Promise(r => setTimeout(r, ms));

const ANALYST_SYSTEM = `
You are a senior marketing data analyst reviewing an autonomous swarm marketing system.
Given metrics across multiple channels and nodes, provide:
1. Top 3 performing channels this period
2. Biggest improvement opportunities
3. Copy/targeting patterns that are winning
4. Recommend what to scale and what to kill
5. Estimated ROI projections for next 7 days

Be specific and data-driven. Return JSON:
{
  "top_channels": [...],
  "opportunities": [...],
  "winning_patterns": [...],
  "scale_recommendations": { "node_type": "reason" },
  "kill_recommendations": { "node_id": "reason" },
  "roi_projection_7d": { "leads": N, "revenue_est": N }
}
`.trim();

export class AnalyticsNode extends BaseNode {
  static nodeType = 'analytics_node';

  static HEARTBEAT_INTERVAL  = 300_000;   // report every 5 min
  static IMPROVEMENT_INTERVAL = 86_400_000;

  constructor(config, region = 'us-east-1', parentId = null) {
    super(config, region, parentId);
    this.reportCadence = (config.report_cadence_hours ?? 24) * 3_600_000;
    this.slackWebhook  = config.slack_webhook_url ?? '';
    this._lastReport   = 0;
  }

  async runCycle() {
    const tasks = await this.receiveTasks(10);
    for (const t of tasks) {
      await this._handleTask(t);
      await this.ackTask(t._receipt_handle);
    }

    const now = Date.now();
    if (now - this._lastReport >= this.reportCadence) {
      await this._runFullReport();
      this._lastReport = now;
    }

    await sleep(60_000); // check every minute
  }

  async _handleTask(task) {
    if (task.command === 'shutdown' && (!task.target_node_id || task.target_node_id === this.nodeId)) {
      await this.stop('commander_shutdown');
    } else if (task.command === 'generate_report') {
      await this._runFullReport();
    } else if (task.command === 'update_config') {
      this.config[task.key] = task.value;
    }
  }

  // ------------------------------------------------------------------ //
  //  FULL REPORT PIPELINE                                                //
  // ------------------------------------------------------------------ //

  async _runFullReport() {
    log.info({ event: 'analytics_report_start', node_id: this.nodeId });

    const [allNodes, campaigns, callStats] = await Promise.all([
      this.memory.getAllNodes(),
      this.memory.getActiveCampaigns(),
      this.memory.getCallStats(),
    ]);

    const since24h  = Math.floor(Date.now() / 1000) - 86400;
    const nodeStats = await this._collectNodeMetrics(allNodes, since24h);
    const report    = this._buildReport(allNodes, campaigns, callStats, nodeStats);

    // Store report to S3
    const date = new Date().toISOString().slice(0, 10);
    const key  = `reports/${date}-${this.nodeId}.json`;
    await this.memory.saveContent(key, JSON.stringify(report, null, 2), 'swarm-exports-store');

    // LLM analysis
    const analysis = await this._analyzeWithLLM(report);
    if (analysis) {
      // Write insights into knowledge base so Commander and nodes can read them
      await this.memory.storeKnowledge(
        'analytics_node',
        'swarm_analysis',
        { report_date: date, analysis, raw_stats: nodeStats },
        analysis.roi_projection_7d?.leads ? Math.min(analysis.roi_projection_7d.leads / 1000, 1) : 0.5,
      );

      if (this.slackWebhook) {
        await this._postToSlack(analysis, report.summary);
      }
    }

    this.increment('reports_generated');
    await this.memory.writeMetric(this.nodeId, this.nodeType, 'reports_generated', 1);
    log.info({ event: 'analytics_report_done', node_id: this.nodeId, s3_key: key });
    return report;
  }

  // ------------------------------------------------------------------ //
  //  METRIC COLLECTION                                                   //
  // ------------------------------------------------------------------ //

  async _collectNodeMetrics(nodes, sinceTs) {
    const result = {};
    const metricNames = [
      'emails_sent', 'human_replies', 'reply_rate',
      'dms_sent', 'calls_made', 'calls_connected', 'human_replies',
      'content_published', 'articles_published',
      'leads_found', 'errors', 'cost_usd',
    ];

    for (const node of nodes) {
      const id = node.node_id;
      result[id] = {
        node_type:  node.node_type,
        status:     node.status,
        generation: node.generation ?? 1,
        metrics:    {},
      };
      for (const name of metricNames) {
        try {
          const rows = await this.memory.getMetrics(id, name, sinceTs);
          if (rows.length) {
            const vals = rows.map(r => r.value);
            result[id].metrics[name] = {
              total: vals.reduce((a, b) => a + b, 0),
              avg:   vals.reduce((a, b) => a + b, 0) / vals.length,
              count: vals.length,
            };
          }
        } catch { /* Skip unavailable metrics */ }
      }
    }
    return result;
  }

  _buildReport(nodes, campaigns, callStats, nodeStats) {
    const running = nodes.filter(n => n.status === 'running');
    const byType  = {};
    for (const n of running) {
      byType[n.node_type] = (byType[n.node_type] ?? 0) + 1;
    }

    const summary = {
      report_time:       new Date().toISOString(),
      total_nodes:       running.length,
      nodes_by_type:     byType,
      active_campaigns:  campaigns.length,
      total_calls:       callStats.total,
      calls_connected:   callStats.connected,
      human_voice_replies: callStats.human_replies,

      // Roll up key metrics across all nodes
      total_emails_sent:   this._sumStat(nodeStats, 'emails_sent'),
      total_dms_sent:      this._sumStat(nodeStats, 'dms_sent'),
      total_leads_found:   this._sumStat(nodeStats, 'leads_found'),
      total_content:       this._sumStat(nodeStats, 'content_published') + this._sumStat(nodeStats, 'articles_published'),
      total_human_replies: this._sumStat(nodeStats, 'human_replies'),
    };

    return { summary, node_stats: nodeStats, campaigns: campaigns.slice(0, 20) };
  }

  _sumStat(nodeStats, metricName) {
    return Object.values(nodeStats).reduce((sum, n) => sum + (n.metrics?.[metricName]?.total ?? 0), 0);
  }

  // ------------------------------------------------------------------ //
  //  LLM ANALYSIS                                                        //
  // ------------------------------------------------------------------ //

  async _analyzeWithLLM(report) {
    const prompt = JSON.stringify({
      summary:    report.summary,
      top_nodes:  Object.entries(report.node_stats)
        .sort((a, b) => (b[1].metrics?.human_replies?.total ?? 0) - (a[1].metrics?.human_replies?.total ?? 0))
        .slice(0, 10)
        .map(([id, data]) => ({ id, ...data })),
    }, null, 2);

    try {
      return await chatJSON({
        system:     ANALYST_SYSTEM,
        messages:   [{ role: 'user', content: prompt }],
        max_tokens: 1024,
      });
    } catch (err) {
      log.error({ event: 'analytics_llm_error', error: err.message });
      return null;
    }
  }

  // ------------------------------------------------------------------ //
  //  SLACK NOTIFICATION                                                  //
  // ------------------------------------------------------------------ //

  async _postToSlack(analysis, summary) {
    try {
      const { default: axios } = await import('axios');
      const text = [
        `*📊 Swarm Daily Report — ${new Date().toISOString().slice(0, 10)}*`,
        `Nodes running: ${summary.total_nodes} | Active campaigns: ${summary.active_campaigns}`,
        `Emails sent: ${summary.total_emails_sent} | DMs sent: ${summary.total_dms_sent} | Calls: ${summary.total_calls}`,
        `Human replies: ${summary.total_human_replies} | Leads found: ${summary.total_leads_found}`,
        `\n*Top channels:* ${(analysis.top_channels ?? []).join(', ')}`,
        `*Scale:* ${Object.keys(analysis.scale_recommendations ?? {}).join(', ')}`,
        `*7d lead projection:* ${analysis.roi_projection_7d?.leads ?? 'N/A'}`,
      ].join('\n');

      await axios.post(this.slackWebhook, { text }, { timeout: 10_000 });
    } catch (err) {
      log.warn({ event: 'slack_post_error', error: err.message });
    }
  }

  // ------------------------------------------------------------------ //
  //  METRICS                                                             //
  // ------------------------------------------------------------------ //

  collectMetrics() {
    return {
      reports_generated: this.getCounter('reports_generated'),
      errors:            this.getCounter('errors'),
    };
  }

  getImprovementContext() {
    return {
      config:         this.config,
      report_cadence: this.reportCadence / 3_600_000,
      slack_enabled:  !!this.slackWebhook,
    };
  }
}
