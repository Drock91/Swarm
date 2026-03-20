#!/usr/bin/env node
/**
 * status.mjs — Local Swarm Dashboard
 *
 * Prints everything running right now, cost, and key metrics.
 *
 * Usage:
 *   node status.mjs            # one-shot print
 *   node status.mjs --watch    # refresh every 30s
 *   node status.mjs --json     # machine-readable output
 *
 * npm run status
 * npm run dashboard            # auto-refresh
 */

import 'dotenv/config';
import chalk      from 'chalk';
import Table      from 'cli-table3';
import { SharedMemory } from './core/shared_memory.mjs';

const WATCH_INTERVAL = 30_000; // 30 seconds
const args           = process.argv.slice(2);
const watchMode      = args.includes('--watch');
const jsonMode       = args.includes('--json');
const region         = process.env.AWS_REGION ?? 'us-east-1';
const memory         = new SharedMemory(region);

// ------------------------------------------------------------------ //
//  COST ESTIMATES (rough AWS pricing)                                  //
// ------------------------------------------------------------------ //
const FARGATE_COST_PER_HOUR = {
  email_node:     0.04,
  seo_node:       0.06,
  dm_node:        0.04,
  voice_node:     0.05,
  content_node:   0.05,
  scraper_node:   0.07,
  analytics_node: 0.04,
  commander:      0.04,
  default:        0.04,
};

function estimateCostSince(startedAt, nodeType) {
  if (!startedAt) return 0;
  const hours    = (Date.now() - new Date(startedAt).getTime()) / 3_600_000;
  const rate     = FARGATE_COST_PER_HOUR[nodeType] ?? FARGATE_COST_PER_HOUR.default;
  return +(hours * rate).toFixed(4);
}

// ------------------------------------------------------------------ //
//  RENDER                                                              //
// ------------------------------------------------------------------ //

async function printStatus() {
  const nodes = await memory.getAllNodes();

  if (jsonMode) {
    console.log(JSON.stringify(nodes, null, 2));
    return;
  }

  console.clear();

  // ── Header ──────────────────────────────────────────────────────── //
  const now     = new Date().toLocaleString();
  const running = nodes.filter(n => n.status === 'running');
  const stopped = nodes.filter(n => n.status === 'stopped');

  console.log(chalk.bold.cyan('\n  ███████╗██╗    ██╗ █████╗ ██████╗ ███╗   ███╗'));
  console.log(chalk.bold.cyan('  ██╔════╝██║    ██║██╔══██╗██╔══██╗████╗ ████║'));
  console.log(chalk.bold.cyan('  ███████╗██║ █╗ ██║███████║██████╔╝██╔████╔██║'));
  console.log(chalk.bold.cyan('  ╚════██║██║███╗██║██╔══██║██╔══██╗██║╚██╔╝██║'));
  console.log(chalk.bold.cyan('  ███████║╚███╔███╔╝██║  ██║██║  ██║██║ ╚═╝ ██║'));
  console.log(chalk.bold.cyan('  ╚══════╝ ╚══╝╚══╝ ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝'));
  console.log(chalk.gray(`\n  ${now}   Region: ${region}`));
  console.log(chalk.green(`  ● Running: ${running.length}`) + '   ' + chalk.red(`■ Stopped: ${stopped.length}`));
  console.log();

  if (!nodes.length) {
    console.log(chalk.yellow('  No nodes registered. Launch nodes with: node run.mjs <node_type>'));
    return;
  }

  // ── Nodes Table ─────────────────────────────────────────────────── //
  const table = new Table({
    head: [
      chalk.bold('NODE ID'),
      chalk.bold('TYPE'),
      chalk.bold('STATUS'),
      chalk.bold('GEN'),
      chalk.bold('UPTIME'),
      chalk.bold('LEADS'),
      chalk.bold('EMAILS SENT'),
      chalk.bold('DMs SENT'),
      chalk.bold('CALLS MADE'),
      chalk.bold('HUMAN REPLIES'),
      chalk.bold('REPLY RATE'),
      chalk.bold('ERRORS'),
      chalk.bold('EST COST $'),
    ],
    colWidths: [20, 16, 10, 5, 10, 8, 12, 9, 11, 14, 11, 8, 11],
    style: { head: [], border: ['gray'] },
  });

  let totalCost      = 0;
  let totalLeads     = 0;
  let totalEmails    = 0;
  let totalDMs       = 0;
  let totalCalls     = 0;
  let totalReplies   = 0;
  let totalErrors    = 0;

  const sorted = [...nodes].sort((a, b) => {
    const order = { running: 0, stopped: 1 };
    return (order[a.status] ?? 2) - (order[b.status] ?? 2);
  });

  for (const node of sorted) {
    const m       = node.metrics_summary ?? {};
    const cost    = estimateCostSince(node.started_at, node.node_type);
    const uptime  = node.started_at ? _humanDuration(Date.now() - new Date(node.started_at).getTime()) : '—';
    const status  = node.status === 'running' ? chalk.green('running') : chalk.red('stopped');

    const emails    = _fmt(m.emails_sent      ?? 0);
    const dms       = _fmt(m.dms_sent         ?? 0);
    const calls     = _fmt(m.calls_made       ?? 0);
    const leads     = _fmt(m.leads_found      ?? m.leads_in_db ?? 0);
    const replies   = _fmt(m.human_replies    ?? 0);
    const replyRate = m.reply_rate ? `${(m.reply_rate * 100).toFixed(1)}%` : '—';
    const errors    = _fmt(m.errors ?? 0);
    const costStr   = node.status === 'running' ? chalk.yellow(`$${cost}`) : chalk.gray('—');

    if (node.status === 'running') {
      totalCost    += cost;
      totalLeads   += m.leads_found ?? m.leads_in_db ?? 0;
      totalEmails  += m.emails_sent ?? 0;
      totalDMs     += m.dms_sent ?? 0;
      totalCalls   += m.calls_made ?? 0;
      totalReplies += m.human_replies ?? 0;
      totalErrors  += m.errors ?? 0;
    }

    table.push([
      chalk.cyan(node.node_id.slice(0, 19)),
      node.node_type,
      status,
      node.generation ?? 1,
      uptime,
      leads,
      emails,
      dms,
      calls,
      replies,
      replyRate,
      errors > 0 ? chalk.red(errors) : errors,
      costStr,
    ]);
  }

  console.log(table.toString());

  // ── Totals Summary ──────────────────────────────────────────────── //
  const replyRateTotal = totalEmails + totalDMs + totalCalls > 0
    ? ((totalReplies / (totalEmails + totalDMs + totalCalls)) * 100).toFixed(2) + '%'
    : '—';

  console.log(chalk.bold('\n  ── TOTALS (running nodes) ──────────────────────────────────'));
  console.log(`  ${chalk.bold('Leads in pipeline:')}  ${chalk.green(_fmt(totalLeads))}`);
  console.log(`  ${chalk.bold('Emails sent:      ')}  ${chalk.green(_fmt(totalEmails))}`);
  console.log(`  ${chalk.bold('DMs sent:         ')}  ${chalk.green(_fmt(totalDMs))}`);
  console.log(`  ${chalk.bold('Calls made:       ')}  ${chalk.green(_fmt(totalCalls))}`);
  console.log(`  ${chalk.bold('Human replies:    ')}  ${chalk.green(_fmt(totalReplies))}  ${chalk.gray('(non-auto)')}`);
  console.log(`  ${chalk.bold('Overall reply rate:')} ${chalk.green(replyRateTotal)}`);
  console.log(`  ${chalk.bold('Errors:           ')}  ${totalErrors > 0 ? chalk.red(_fmt(totalErrors)) : chalk.green('0')}`);
  console.log(`  ${chalk.bold('Est. session cost: ')} ${chalk.yellow('$' + totalCost.toFixed(4))}`);
  console.log();

  // ── Active Campaigns ────────────────────────────────────────────── //
  try {
    const campaigns = await memory.getActiveCampaigns();
    if (campaigns.length) {
      console.log(chalk.bold('  ── ACTIVE CAMPAIGNS ────────────────────────────────────────'));
      const ct = new Table({
        head: [chalk.bold('CAMPAIGN ID'), chalk.bold('NAME'), chalk.bold('CHANNEL'), chalk.bold('STATUS'), chalk.bold('BUDGET/DAY')],
        colWidths: [38, 28, 16, 10, 12],
        style: { border: ['gray'] },
      });
      for (const c of campaigns.slice(0, 15)) {
        ct.push([
          c.campaign_id?.slice(0, 36) ?? '—',
          (c.name ?? 'Unnamed').slice(0, 27),
          c.node_type ?? '—',
          chalk.green(c.status ?? '—'),
          c.daily_budget_usd != null ? `$${c.daily_budget_usd}` : '—',
        ]);
      }
      console.log(ct.toString());
    }
  } catch { /* DynamoDB may not be up in dev */ }

  if (watchMode) {
    console.log(chalk.gray(`\n  Auto-refreshing every ${WATCH_INTERVAL / 1000}s... Press Ctrl+C to exit.\n`));
  }
}

// ------------------------------------------------------------------ //
//  HELPERS                                                             //
// ------------------------------------------------------------------ //

function _fmt(n) {
  return Number(n).toLocaleString();
}

function _humanDuration(ms) {
  const s = Math.floor(ms / 1000);
  if (s < 60)   return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  return `${Math.floor(s / 86400)}d ${Math.floor((s % 86400) / 3600)}h`;
}

// ------------------------------------------------------------------ //
//  MAIN                                                                //
// ------------------------------------------------------------------ //

await printStatus();

if (watchMode) {
  setInterval(async () => {
    try { await printStatus(); } catch (err) {
      console.error(chalk.red('Dashboard error:'), err.message);
    }
  }, WATCH_INTERVAL);
}
