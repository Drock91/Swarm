#!/usr/bin/env node
/**
 * infra/teardown.mjs — Remove all Swarm AWS resources.
 *
 * WARNING: This is destructive! All data will be deleted.
 *
 * Usage:
 *   node infra/teardown.mjs
 *   node infra/teardown.mjs --confirm   (skip interactive prompt)
 */

import 'dotenv/config';
import readline from 'readline';
import {
  DynamoDBClient,
  DeleteTableCommand,
  DescribeTableCommand,
} from '@aws-sdk/client-dynamodb';
import {
  S3Client,
  DeleteBucketCommand,
  ListObjectVersionsCommand,
  DeleteObjectsCommand,
} from '@aws-sdk/client-s3';
import {
  SQSClient,
  GetQueueUrlCommand,
  DeleteQueueCommand,
} from '@aws-sdk/client-sqs';
import {
  ECSClient,
  DeleteClusterCommand,
  ListTasksCommand,
  StopTaskCommand,
  DescribeClustersCommand,
} from '@aws-sdk/client-ecs';

const REGION = process.env.AWS_REGION ?? 'us-east-1';
const DDB    = new DynamoDBClient({ region: REGION });
const S3     = new S3Client({ region: REGION });
const SQS    = new SQSClient({ region: REGION });
const ECS    = new ECSClient({ region: REGION });

const TABLES  = ['swarm-nodes','swarm-leads','swarm-campaigns','swarm-metrics','swarm-knowledge','swarm-calls','swarm-costs'];
const BUCKETS = ['swarm-content-store','swarm-exports-store','swarm-models-store'];
const QUEUES  = ['swarm-commander-queue','swarm-email-queue','swarm-seo-queue','swarm-scraper-queue','swarm-analytics-queue'];
const CLUSTER = 'swarm-cluster';

function done(msg)  { console.log(`  🗑   ${msg}`); }
function skip(msg)  { console.log(`  ⏭   ${msg} (not found)`); }
function warn(msg)  { console.log(`  ⚠️   ${msg}`); }

// ------------------------------------------------------------------ //
//  Confirm                                                             //
// ------------------------------------------------------------------ //

async function confirm() {
  if (process.argv.includes('--confirm')) return true;
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  return new Promise(resolve => {
    rl.question(
      '\n⚠️  This will DELETE all Swarm AWS resources and data. Type "yes" to continue: ',
      ans => { rl.close(); resolve(ans.trim().toLowerCase() === 'yes'); }
    );
  });
}

// ------------------------------------------------------------------ //
//  Purge + delete S3 bucket (versioned)                               //
// ------------------------------------------------------------------ //

async function purgeBucket(bucket) {
  let keyMarker, versionIdMarker;
  do {
    const res = await S3.send(new ListObjectVersionsCommand({
      Bucket: bucket,
      KeyMarker: keyMarker,
      VersionIdMarker: versionIdMarker,
    }));
    const objects = [
      ...(res.Versions ?? []),
      ...(res.DeleteMarkers ?? []),
    ].map(o => ({ Key: o.Key, VersionId: o.VersionId }));

    if (objects.length > 0) {
      await S3.send(new DeleteObjectsCommand({
        Bucket: bucket,
        Delete: { Objects: objects, Quiet: true },
      }));
    }

    keyMarker       = res.NextKeyMarker;
    versionIdMarker = res.NextVersionIdMarker;
  } while (keyMarker || versionIdMarker);
}

// ------------------------------------------------------------------ //
//  Main teardown                                                       //
// ------------------------------------------------------------------ //

console.log('\n🔥  Swarm Teardown\n');

const confirmed = await confirm();
if (!confirmed) {
  console.log('\n  Aborted.\n');
  process.exit(0);
}

console.log('\n  Proceeding with teardown...\n');

// DynamoDB
for (const t of TABLES) {
  try {
    await DDB.send(new DeleteTableCommand({ TableName: t }));
    done(`Deleted DynamoDB table: ${t}`);
  } catch {
    skip(`DynamoDB: ${t}`);
  }
}

console.log();

// S3
for (const b of BUCKETS) {
  try {
    await purgeBucket(b);
    await S3.send(new DeleteBucketCommand({ Bucket: b }));
    done(`Deleted S3 bucket: ${b}`);
  } catch {
    skip(`S3: ${b}`);
  }
}

console.log();

// SQS
for (const q of QUEUES) {
  try {
    const { QueueUrl } = await SQS.send(new GetQueueUrlCommand({ QueueName: q }));
    await SQS.send(new DeleteQueueCommand({ QueueUrl }));
    done(`Deleted SQS queue: ${q}`);
  } catch {
    skip(`SQS: ${q}`);
  }
}

console.log();

// ECS — stop all tasks first
try {
  const { taskArns } = await ECS.send(new ListTasksCommand({ cluster: CLUSTER }));
  if (taskArns?.length) {
    warn(`Stopping ${taskArns.length} running ECS task(s)...`);
    for (const ta of taskArns) {
      await ECS.send(new StopTaskCommand({ cluster: CLUSTER, task: ta, reason: 'Teardown' }));
    }
  }
  await ECS.send(new DeleteClusterCommand({ cluster: CLUSTER }));
  done(`Deleted ECS cluster: ${CLUSTER}`);
} catch {
  skip(`ECS cluster: ${CLUSTER}`);
}

console.log('\n✅  Teardown complete.\n');
