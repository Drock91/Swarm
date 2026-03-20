#!/usr/bin/env node
/**
 * cli.mjs — Swarm Control Interface
 *
 * Interactive REPL to manage all running containers and nodes.
 *
 * Usage:
 *   node cli.mjs
 *
 * Commands:
 *   list                    — list all swarm nodes + containers
 *   logs <name|id>          — tail live logs from a container
 *   kill <node_id|name>     — stop + remove a container, deregister node
 *   spawn <node_type>       — manually spawn a new clone
 *   pause <campaign_id>     — pause a campaign
 *   improve <node_id>       — trigger immediate self-improvement cycle
 *   up                      — docker compose up -d (start everything)
 *   down                    — docker compose down (stop everything)
 *   restart <name>          — restart a specific container
 *   status                  — print dashboard
 *   help                    — show this list
 *   exit / quit             — close
 */

import 'dotenv/config';
import readline from 'readline';
import { execSync, spawn } from 'child_process';
import chalk  from 'chalk';
import Docker from 'dockerode';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import { SharedMemory } from './core/shared_memory.mjs';
import { randomUUID }   from 'crypto';

const region = process.env.AWS_REGION ?? 'us-east-1';
const memory = new SharedMemory(region);
const sqs    = new SQSClient({ region });

const DOCKER_OPTS = process.platform === 'win32'
  ? { socketPath: '//./pipe/docker_engine' }
  : { socketPath: '/var/run/docker.sock' };
const docker = new Docker(DOCKER_OPTS);

const QUEUE_MAP = {
  email_node:     process.env.SWARM_EMAIL_QUEUE_URL,
  seo_node:       process.env.SWARM_SEO_QUEUE_URL,
  scraper_node:   process.env.SWARM_SCRAPER_QUEUE_URL,
  analytics_node: process.env.SWARM_ANALYTICS_QUEUE_URL,
  commander:      process.env.SWARM_COMMANDER_QUEUE_URL,
};

const DOCKER_IMAGE  = process.env.SWARM_DOCKER_IMAGE   ?? 'the-swarm-node';
const DOCKER_NETWORK = process.env.SWARM_DOCKER_NETWORK ?? 'the-swarm_swarm-net';

// ------------------------------------------------------------------ //
//  HELPERS                                                             //
// ------------------------------------------------------------------ //

async function getSwarmContainers() {
  const all = await docker.listContainers({ all: true });
  return all.filter(c => c.Names.some(n => n.includes('swarm')));
}

function findContainer(containers, query) {
  const q = query.toLowerCase();
  return containers.find(c =>
    c.Id.startsWith(query) ||
    c.Names.some(n => n.replace('/', '').toLowerCase() === q)
  );
}

async function sendNodeCommand(nodeType, payload) {
  const url = QUEUE_MAP[nodeType];
  if (!url) { console.log(chalk.red(`  No queue URL for ${nodeType}`)); return; }
  await sqs.send(new SendMessageCommand({
    QueueUrl:    url,
    MessageBody: JSON.stringify(payload),
  }));
  console.log(chalk.green(`  ✓ Command sent to ${nodeType} queue`));
}

// ------------------------------------------------------------------ //
//  COMMANDS                                                            //
// ------------------------------------------------------------------ //

async function cmdList() {
  const [nodes, containers] = await Promise.all([
    memory.getAllNodes(),
    getSwarmContainers().catch(() => []),
  ]);

  console.log(chalk.bold.cyan('\n  ── NODES (DynamoDB) ──'));
  if (!nodes.length) {
    console.log(chalk.gray('  none registered'));
  } else {
    for (const n of nodes) {
      const s = n.status === 'running' ? chalk.green('● running') : chalk.red('■ stopped');
      const gen = n.generation ? chalk.gray(`gen${n.generation}`) : '';
      const email = n.sender_email ? chalk.gray(`<${n.sender_email}>`) : '';
      console.log(`  ${s}  ${chalk.cyan(n.node_id.padEnd(34))}  ${n.node_type.padEnd(16)} ${gen} ${email}`);
    }
  }

  console.log(chalk.bold.cyan('\n  ── DOCKER CONTAINERS ──'));
  if (!containers.length) {
    console.log(chalk.gray('  none found'));
  } else {
    for (const c of containers) {
      const s     = c.State === 'running' ? chalk.green('● running') : chalk.red('■ stopped');
      const name  = c.Names[0]?.replace('/', '') ?? '?';
      console.log(`  ${s}  ${name.padEnd(30)}  ${c.Id.slice(0, 12)}  ${chalk.gray(c.Status)}`);
    }
  }
  console.log();
}

async function cmdLogs(args) {
  const query = args[0];
  if (!query) { console.log(chalk.red('  Usage: logs <container_name_or_id>')); return; }
  const all   = await getSwarmContainers();
  const found = findContainer(all, query);
  if (!found) { console.log(chalk.red(`  Container not found: ${query}`)); return; }

  console.log(chalk.gray(`\n  Tailing logs for ${found.Names[0]}... (Ctrl+C to stop)\n`));
  const c = docker.getContainer(found.Id);
  const stream = await c.logs({ stdout: true, stderr: true, follow: true, tail: 50 });
  docker.modem.demuxStream(stream, process.stdout, process.stderr);
}

async function cmdKill(args) {
  const query = args[0];
  if (!query) { console.log(chalk.red('  Usage: kill <node_id_or_container_name>')); return; }

  // Try to deregister from DynamoDB
  try { await memory.deregisterNode(query, 'cli_kill'); } catch { /* not registered */ }

  // Stop + remove Docker container
  const all   = await getSwarmContainers();
  const found = findContainer(all, query);
  if (found) {
    const c = docker.getContainer(found.Id);
    await c.stop({ t: 5 }).catch(() => {});
    await c.remove().catch(() => {});
    console.log(chalk.green(`  ✓ Container stopped and removed: ${found.Names[0]}`));
  } else {
    console.log(chalk.yellow(`  No Docker container found for "${query}" (may already be gone)`));
  }
}

async function cmdSpawn(args) {
  const nodeType = args[0];
  if (!nodeType) { console.log(chalk.red('  Usage: spawn <node_type>  (e.g. spawn email_node)')); return; }

  const validTypes = ['email_node', 'scraper_node', 'seo_node', 'analytics_node'];
  if (!validTypes.includes(nodeType)) {
    console.log(chalk.red(`  Unknown type. Valid: ${validTypes.join(', ')}`)); return;
  }

  const nodeId = `${nodeType.replace('_node', '')}-clone-${randomUUID().slice(0, 6)}`;
  const env = [
    `NODE_TYPE=${nodeType}`,
    `NODE_ID=${nodeId}`,
    `AWS_REGION=${region}`,
    `AWS_ACCESS_KEY_ID=${process.env.AWS_ACCESS_KEY_ID ?? ''}`,
    `AWS_SECRET_ACCESS_KEY=${process.env.AWS_SECRET_ACCESS_KEY ?? ''}`,
    `OPENAI_API_KEY=${process.env.OPENAI_API_KEY ?? ''}`,
    `SENDGRID_API_KEY=${process.env.SENDGRID_API_KEY ?? ''}`,
    `SENDGRID_FROM_EMAIL=${args[1] ?? process.env.SENDGRID_FROM_EMAIL_1 ?? ''}`,
    `SENDGRID_FROM_NAME=${process.env.SENDGRID_FROM_NAME_1 ?? ''}`,
    ...Object.entries(process.env)
      .filter(([k]) => k.startsWith('SWARM_') && k.endsWith('_QUEUE_URL'))
      .map(([k, v]) => `${k}=${v}`),
  ];

  try {
    const container = await docker.createContainer({
      Image:      DOCKER_IMAGE,
      name:       nodeId,
      Env:        env,
      HostConfig: { NetworkMode: DOCKER_NETWORK, RestartPolicy: { Name: 'unless-stopped' } },
    });
    await container.start();
    await memory.registerNode(nodeId, nodeType, { container_id: container.id, generation: 1 });
    console.log(chalk.green(`  ✓ Spawned ${nodeType} as "${nodeId}" (${container.id.slice(0, 12)})`));
  } catch (err) {
    console.log(chalk.red(`  ✗ Failed: ${err.message}`));
  }
}

async function cmdImprove(args) {
  const nodeId = args[0];
  if (!nodeId) { console.log(chalk.red('  Usage: improve <node_id>')); return; }
  const node = await memory.getNode(nodeId);
  if (!node) { console.log(chalk.red(`  Node not found: ${nodeId}`)); return; }
  await sendNodeCommand(node.node_type, { command: 'trigger_improvement', target_node_id: nodeId });
}

async function cmdPause(args) {
  const cid = args[0];
  if (!cid) { console.log(chalk.red('  Usage: pause <campaign_id>')); return; }
  await memory.pauseCampaign(cid);
  console.log(chalk.green(`  ✓ Campaign paused: ${cid}`));
}

function cmdCompose(subCmd) {
  const cmd = subCmd === 'down' ? 'docker compose down' : 'docker compose up -d --build';
  console.log(chalk.gray(`\n  Running: ${cmd}\n`));
  try {
    execSync(cmd, { stdio: 'inherit', cwd: process.cwd() });
  } catch (err) {
    console.log(chalk.red(`  ✗ ${err.message}`));
  }
}

async function cmdRestart(args) {
  const query = args[0];
  if (!query) { console.log(chalk.red('  Usage: restart <container_name_or_id>')); return; }
  const all   = await getSwarmContainers();
  const found = findContainer(all, query);
  if (!found) { console.log(chalk.red(`  Not found: ${query}`)); return; }
  await docker.getContainer(found.Id).restart();
  console.log(chalk.green(`  ✓ Restarted: ${found.Names[0]}`));
}

function printHelp() {
  console.log(`
  ${chalk.bold.cyan('Swarm CLI Commands')}

  ${chalk.bold('list')}                     list all nodes and Docker containers
  ${chalk.bold('logs')} <name>              tail live container logs
  ${chalk.bold('kill')} <node_id|name>      stop + remove container, deregister node
  ${chalk.bold('spawn')} <type> [email]     manually spawn a clone container
  ${chalk.bold('restart')} <name>           restart a container
  ${chalk.bold('improve')} <node_id>        trigger immediate self-improvement cycle
  ${chalk.bold('pause')} <campaign_id>      pause a campaign
  ${chalk.bold('up')}                       docker compose up -d (start all services)
  ${chalk.bold('down')}                     docker compose down (stop all services)
  ${chalk.bold('status')}                   open the metrics dashboard
  ${chalk.bold('help')}                     show this menu
  ${chalk.bold('exit')}                     quit

  ${chalk.gray('Valid node types:')}  email_node  scraper_node  seo_node  analytics_node
  `);
}

// ------------------------------------------------------------------ //
//  REPL                                                                //
// ------------------------------------------------------------------ //

const rl = readline.createInterface({
  input:  process.stdin,
  output: process.stdout,
  prompt: chalk.bold.cyan('swarm> '),
});

console.log(chalk.bold.cyan('\n  Swarm Control Interface'));
console.log(chalk.gray('  Type "help" for commands, "exit" to quit.\n'));
rl.prompt();

rl.on('line', async (line) => {
  const parts   = line.trim().split(/\s+/);
  const cmd     = parts[0]?.toLowerCase();
  const args    = parts.slice(1);

  try {
    switch (cmd) {
      case 'list':     await cmdList();           break;
      case 'logs':     await cmdLogs(args);       break;
      case 'kill':     await cmdKill(args);       break;
      case 'spawn':    await cmdSpawn(args);      break;
      case 'improve':  await cmdImprove(args);    break;
      case 'pause':    await cmdPause(args);      break;
      case 'up':       cmdCompose('up');          break;
      case 'down':     cmdCompose('down');        break;
      case 'restart':  await cmdRestart(args);   break;
      case 'status':   execSync('node status.mjs', { stdio: 'inherit' }); break;
      case 'help':     printHelp();              break;
      case '':                                   break;
      case 'exit':
      case 'quit':
        console.log(chalk.gray('\n  bye\n'));
        process.exit(0);
        break;
      default:
        console.log(chalk.red(`  Unknown command: "${cmd}". Type "help" for options.`));
    }
  } catch (err) {
    console.log(chalk.red(`  Error: ${err.message}`));
  }

  rl.prompt();
});

rl.on('close', () => process.exit(0));
