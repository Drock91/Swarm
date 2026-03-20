#!/usr/bin/env node
/**
 * infra/deploy.mjs — Bootstrap all AWS resources the Swarm needs.
 *
 * Run once before `npm start`:
 *   node infra/deploy.mjs
 *
 * AWS resources created:
 *   DynamoDB tables: swarm-nodes, swarm-leads, swarm-campaigns,
 *                    swarm-metrics, swarm-knowledge, swarm-calls, swarm-costs
 *   S3 buckets:      swarm-content-store, swarm-exports-store, swarm-models-store
 *   SQS queues:      one queue per node type (8 total)
 *   ECS:             cluster "swarm-cluster"
 */

import 'dotenv/config';
import {
  DynamoDBClient,
  CreateTableCommand,
  DescribeTableCommand,
  ResourceInUseException,
} from '@aws-sdk/client-dynamodb';
import {
  S3Client,
  CreateBucketCommand,
  HeadBucketCommand,
  PutBucketVersioningCommand,
} from '@aws-sdk/client-s3';
import {
  SQSClient,
  CreateQueueCommand,
  GetQueueUrlCommand,
} from '@aws-sdk/client-sqs';
import {
  ECSClient,
  CreateClusterCommand,
  DescribeClustersCommand,
} from '@aws-sdk/client-ecs';
import {
  IAMClient,
  CreateServiceLinkedRoleCommand,
} from '@aws-sdk/client-iam';

const REGION  = process.env.AWS_REGION ?? 'us-east-1';
const ACCOUNT = process.env.AWS_ACCOUNT_ID;   // optional, for tagging
const DDB     = new DynamoDBClient({ region: REGION });
const S3      = new S3Client({ region: REGION });
const SQS     = new SQSClient({ region: REGION });
const ECS     = new ECSClient({ region: REGION });
const IAM     = new IAMClient({ region: REGION });

// ------------------------------------------------------------------ //
//  DynamoDB table definitions                                          //
// ------------------------------------------------------------------ //

const TABLES = [
  {
    TableName:             'swarm-nodes',
    BillingMode:           'PAY_PER_REQUEST',
    KeySchema:             [{ AttributeName: 'node_id', KeyType: 'HASH' }],
    AttributeDefinitions:  [{ AttributeName: 'node_id', AttributeType: 'S' }],
  },
  {
    TableName:             'swarm-leads',
    BillingMode:           'PAY_PER_REQUEST',
    KeySchema:             [{ AttributeName: 'lead_id', KeyType: 'HASH' }],
    AttributeDefinitions:  [{ AttributeName: 'lead_id', AttributeType: 'S' }],
  },
  {
    TableName:             'swarm-campaigns',
    BillingMode:           'PAY_PER_REQUEST',
    KeySchema:             [{ AttributeName: 'campaign_id', KeyType: 'HASH' }],
    AttributeDefinitions:  [{ AttributeName: 'campaign_id', AttributeType: 'S' }],
  },
  {
    TableName:             'swarm-metrics',
    BillingMode:           'PAY_PER_REQUEST',
    KeySchema: [
      { AttributeName: 'node_id',   KeyType: 'HASH' },
      { AttributeName: 'metric_ts', KeyType: 'RANGE' },
    ],
    AttributeDefinitions: [
      { AttributeName: 'node_id',   AttributeType: 'S' },
      { AttributeName: 'metric_ts', AttributeType: 'S' },
    ],
  },
  {
    TableName:             'swarm-knowledge',
    BillingMode:           'PAY_PER_REQUEST',
    KeySchema: [
      { AttributeName: 'source_node', KeyType: 'HASH' },
      { AttributeName: 'stored_at',   KeyType: 'RANGE' },
    ],
    AttributeDefinitions: [
      { AttributeName: 'source_node', AttributeType: 'S' },
      { AttributeName: 'stored_at',   AttributeType: 'S' },
    ],
  },
  {
    TableName:             'swarm-calls',
    BillingMode:           'PAY_PER_REQUEST',
    KeySchema:             [{ AttributeName: 'call_id', KeyType: 'HASH' }],
    AttributeDefinitions:  [{ AttributeName: 'call_id', AttributeType: 'S' }],
  },
  {
    TableName:             'swarm-costs',
    BillingMode:           'PAY_PER_REQUEST',
    KeySchema: [
      { AttributeName: 'service',    KeyType: 'HASH' },
      { AttributeName: 'timestamp',  KeyType: 'RANGE' },
    ],
    AttributeDefinitions: [
      { AttributeName: 'service',   AttributeType: 'S' },
      { AttributeName: 'timestamp', AttributeType: 'S' },
    ],
  },
];

// ------------------------------------------------------------------ //
//  S3 buckets                                                          //
// ------------------------------------------------------------------ //

const BUCKETS = [
  'swarm-content-store',
  'swarm-exports-store',
  'swarm-models-store',
];

// ------------------------------------------------------------------ //
//  SQS queues (one per node type + commander)                          //
// ------------------------------------------------------------------ //

const QUEUES = [
  'swarm-commander-queue',
  'swarm-email-queue',
  'swarm-seo-queue',

  'swarm-scraper-queue',
  'swarm-analytics-queue',
];

// ------------------------------------------------------------------ //
//  Helpers                                                             //
// ------------------------------------------------------------------ //

function ok(msg)  { console.log(`  ✅  ${msg}`); }
function skip(msg) { console.log(`  ⏭   ${msg} (already exists)`); }
function info(msg) { console.log(`  ℹ️   ${msg}`); }

async function ensureTable(def) {
  try {
    await DDB.send(new CreateTableCommand(def));
    ok(`Created DynamoDB table: ${def.TableName}`);
  } catch (e) {
    if (e.name === 'ResourceInUseException' || e.__type?.includes('ResourceInUse')) {
      skip(`DynamoDB: ${def.TableName}`);
    } else {
      throw e;
    }
  }
}

async function ensureBucket(name) {
  try {
    await S3.send(new HeadBucketCommand({ Bucket: name }));
    skip(`S3: ${name}`);
    return;
  } catch {/* doesn't exist */}

  const params = { Bucket: name };
  if (REGION !== 'us-east-1') {
    params.CreateBucketConfiguration = { LocationConstraint: REGION };
  }
  await S3.send(new CreateBucketCommand(params));
  // Enable versioning
  await S3.send(new PutBucketVersioningCommand({
    Bucket: name,
    VersioningConfiguration: { Status: 'Enabled' },
  }));
  ok(`Created S3 bucket: ${name}`);
}

async function ensureQueue(name) {
  try {
    await SQS.send(new GetQueueUrlCommand({ QueueName: name }));
    skip(`SQS: ${name}`);
    return;
  } catch {/* doesn't exist */}

  const res = await SQS.send(new CreateQueueCommand({
    QueueName: name,
    Attributes: {
      VisibilityTimeout: '300',
      MessageRetentionPeriod: '86400',
    },
  }));
  ok(`Created SQS queue: ${name}  →  ${res.QueueUrl}`);
}

async function ensureEcsCluster(name) {
  // Ensure the ECS service-linked role exists
  try {
    await IAM.send(new CreateServiceLinkedRoleCommand({
      AWSServiceName: 'ecs.amazonaws.com',
    }));
    info('Created ECS service-linked role');
  } catch (e) {
    if (!e.message?.includes('has been taken') && !e.message?.includes('already exists')) throw e;
    // role already exists – fine
  }

  const desc = await ECS.send(new DescribeClustersCommand({ clusters: [name] }));
  const existing = desc.clusters?.find(c => c.clusterName === name && c.status !== 'INACTIVE');
  if (existing) {
    skip(`ECS cluster: ${name}`);
    return;
  }
  await ECS.send(new CreateClusterCommand({ clusterName: name }));
  ok(`Created ECS cluster: ${name}`);
}

// ------------------------------------------------------------------ //
//  Main                                                                //
// ------------------------------------------------------------------ //

console.log('\n🚀  Deploying Swarm infrastructure...\n');
console.log(`  Region: ${REGION}`);
if (ACCOUNT) console.log(`  Account: ${ACCOUNT}`);
console.log();

info('Creating DynamoDB tables...');
for (const t of TABLES) await ensureTable(t);

console.log();
info('Creating S3 buckets...');
for (const b of BUCKETS) await ensureBucket(b);

console.log();
info('Creating SQS queues...');
for (const q of QUEUES) await ensureQueue(q);

console.log();
info('Creating ECS cluster...');
await ensureEcsCluster('swarm-cluster');

console.log('\n✅  Deployment complete!\n');
console.log('  Next steps:');
console.log('    1. Copy .env.example → .env  and fill in your API keys');
console.log('    2. Build and push Docker images to ECR for each node type');
console.log('    3. Register ECS task definitions for each node type');
console.log('    4. Run:  node swarm.mjs                (start the Commander)');
console.log('    5. Run:  node status.mjs --watch       (watch the dashboard)');
console.log();
