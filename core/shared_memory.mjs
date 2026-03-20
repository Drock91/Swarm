/**
 * Shared Memory Layer — DynamoDB + S3 abstraction for the Swarm.
 *
 * Every node reads/writes through here. This is the collective brain.
 * Tables:
 *   swarm-nodes      — running node registry + heartbeat
 *   swarm-leads      — unified lead database across all nodes
 *   swarm-campaigns  — campaign configs, copy, targeting
 *   swarm-metrics    — per-node performance metrics (rolling 90d)
 *   swarm-knowledge  — winning patterns, copy, strategies
 *   swarm-calls      — voice call records
 *   swarm-costs      — AWS cost attribution per node
 *
 * S3 Buckets:
 *   swarm-content-store  — generated blog posts, threads, copy
 *   swarm-exports-store  — analytics exports, reports
 *   swarm-models-store   — fine-tuned prompt templates, winning variants
 */

import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  PutCommand,
  GetCommand,
  UpdateCommand,
  QueryCommand,
  ScanCommand,
} from '@aws-sdk/lib-dynamodb';
import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  ListObjectsV2Command,
} from '@aws-sdk/client-s3';
import { randomUUID } from 'crypto';
import { log } from './logger.mjs';

const TABLES = {
  nodes:     'swarm-nodes',
  leads:     'swarm-leads',
  campaigns: 'swarm-campaigns',
  metrics:   'swarm-metrics',
  knowledge: 'swarm-knowledge',
  calls:     'swarm-calls',
  costs:     'swarm-costs',
};

const CONTENT_BUCKET = 'swarm-content-store';
const EXPORTS_BUCKET = 'swarm-exports-store';
const MODELS_BUCKET  = 'swarm-models-store';

export class SharedMemory {
  /** @param {string} region */
  constructor(region = 'us-east-1') {
    this.region = region;
    const ddbClient = new DynamoDBClient({ region });
    // DynamoDBDocumentClient handles JS <-> DynamoDB type marshaling automatically
    this.ddb = DynamoDBDocumentClient.from(ddbClient, {
      marshallOptions:   { removeUndefinedValues: true, convertEmptyValues: false },
      unmarshallOptions: { wrapNumbers: false },
    });
    this.s3 = new S3Client({ region });
  }

  _table(key) {
    return TABLES[key];
  }

  // ------------------------------------------------------------------ //
  //  NODE REGISTRY                                                       //
  // ------------------------------------------------------------------ //

  async registerNode(nodeId, nodeType, config) {
    await this.ddb.send(new PutCommand({
      TableName: this._table('nodes'),
      Item: {
        node_id:         nodeId,
        node_type:       nodeType,
        status:          'running',
        started_at:      new Date().toISOString(),
        last_heartbeat:  Math.floor(Date.now() / 1000),
        config,
        generation:      config.generation ?? 1,
        parent_id:       config.parent_id ?? null,
        metrics_summary: {},
      },
    }));
    log.info({ event: 'node_registered', node_id: nodeId, node_type: nodeType });
  }

  async heartbeat(nodeId, metricsSummary) {
    await this.ddb.send(new UpdateCommand({
      TableName:                 this._table('nodes'),
      Key:                       { node_id: nodeId },
      UpdateExpression:          'SET last_heartbeat = :ts, metrics_summary = :m, #st = :s',
      ExpressionAttributeNames:  { '#st': 'status' },
      ExpressionAttributeValues: {
        ':ts': Math.floor(Date.now() / 1000),
        ':m':  metricsSummary,
        ':s':  'running',
      },
    }));
  }

  async deregisterNode(nodeId, reason = 'shutdown') {
    await this.ddb.send(new UpdateCommand({
      TableName:                 this._table('nodes'),
      Key:                       { node_id: nodeId },
      UpdateExpression:          'SET #st = :s, stopped_at = :t, stop_reason = :r',
      ExpressionAttributeNames:  { '#st': 'status' },
      ExpressionAttributeValues: {
        ':s': 'stopped',
        ':t': new Date().toISOString(),
        ':r': reason,
      },
    }));
  }

  async getAllNodes(nodeType = null) {
    const params = { TableName: this._table('nodes') };
    if (nodeType) {
      params.FilterExpression          = 'node_type = :t';
      params.ExpressionAttributeValues = { ':t': nodeType };
    }
    const resp = await this.ddb.send(new ScanCommand(params));
    return resp.Items ?? [];
  }

  async getNode(nodeId) {
    const resp = await this.ddb.send(new GetCommand({
      TableName: this._table('nodes'),
      Key:       { node_id: nodeId },
    }));
    return resp.Item ?? null;
  }

  // ------------------------------------------------------------------ //
  //  LEADS DATABASE                                                      //
  // ------------------------------------------------------------------ //

  async upsertLead(lead) {
    const leadId = lead.lead_id ?? randomUUID();
    const now    = new Date().toISOString();
    const item   = {
      ...lead,
      lead_id:    leadId,
      created_at: lead.created_at ?? now,
      updated_at: now,
    };
    await this.ddb.send(new PutCommand({ TableName: this._table('leads'), Item: item }));
    return leadId;
  }

  async getLeads(filters = null, limit = 100) {
    const params = { TableName: this._table('leads'), Limit: limit };
    if (filters && Object.keys(filters).length > 0) {
      const exprs = [];
      const vals  = {};
      for (const [k, v] of Object.entries(filters)) {
        const safe = k.replace(/-/g, '_');
        exprs.push(`${k} = :${safe}`);
        vals[`:${safe}`] = v;
      }
      params.FilterExpression          = exprs.join(' AND ');
      params.ExpressionAttributeValues = vals;
    }
    const resp = await this.ddb.send(new ScanCommand(params));
    return resp.Items ?? [];
  }

  async countLeads(sourceNode = null) {
    const params = { TableName: this._table('leads'), Select: 'COUNT' };
    if (sourceNode) {
      params.FilterExpression          = 'source_node = :n';
      params.ExpressionAttributeValues = { ':n': sourceNode };
    }
    const resp = await this.ddb.send(new ScanCommand(params));
    return resp.Count ?? 0;
  }

  // ------------------------------------------------------------------ //
  //  METRICS                                                             //
  // ------------------------------------------------------------------ //

  async writeMetric(nodeId, nodeType, metricName, value, tags = {}) {
    const ts   = Math.floor(Date.now() / 1000);
    const date = new Date().toISOString().slice(0, 10);
    await this.ddb.send(new PutCommand({
      TableName: this._table('metrics'),
      Item: {
        pk:          `${nodeId}#${metricName}`,
        timestamp:   ts,
        node_id:     nodeId,
        node_type:   nodeType,
        metric_name: metricName,
        value,
        tags,
        date,
      },
    }));
  }

  async getMetrics(nodeId, metricName, sinceTs = null) {
    const pk     = `${nodeId}#${metricName}`;
    const params = {
      TableName:                 this._table('metrics'),
      KeyConditionExpression:    'pk = :pk',
      ExpressionAttributeValues: { ':pk': pk },
      ScanIndexForward:          false,
    };
    if (sinceTs) {
      params.KeyConditionExpression              += ' AND #ts >= :ts';
      params.ExpressionAttributeNames             = { '#ts': 'timestamp' };
      params.ExpressionAttributeValues[':ts']     = sinceTs;
    }
    const resp = await this.ddb.send(new QueryCommand(params));
    return resp.Items ?? [];
  }

  // ------------------------------------------------------------------ //
  //  KNOWLEDGE BASE (winning patterns)                                   //
  // ------------------------------------------------------------------ //

  async storeKnowledge(nodeType, patternType, data, score) {
    const kid = randomUUID();
    const now = new Date().toISOString();
    await this.ddb.send(new PutCommand({
      TableName: this._table('knowledge'),
      Item: {
        knowledge_id: kid,
        node_type:    nodeType,
        pattern_type: patternType,
        data,
        score,
        uses:         0,
        wins:         0,
        created_at:   now,
        updated_at:   now,
      },
    }));
    return kid;
  }

  async getTopKnowledge(nodeType, patternType, topN = 10) {
    const resp = await this.ddb.send(new ScanCommand({
      TableName:                 this._table('knowledge'),
      FilterExpression:          'node_type = :nt AND pattern_type = :pt',
      ExpressionAttributeValues: { ':nt': nodeType, ':pt': patternType },
    }));
    const items = resp.Items ?? [];
    return items
      .sort((a, b) => (b.score ?? 0) - (a.score ?? 0))
      .slice(0, topN);
  }

  async incrementKnowledgeWin(knowledgeId) {
    await this.ddb.send(new UpdateCommand({
      TableName:                 this._table('knowledge'),
      Key:                       { knowledge_id: knowledgeId },
      UpdateExpression:          'ADD wins :w, uses :u SET updated_at = :t',
      ExpressionAttributeValues: {
        ':w': 1,
        ':u': 1,
        ':t': new Date().toISOString(),
      },
    }));
  }

  // ------------------------------------------------------------------ //
  //  CAMPAIGNS                                                           //
  // ------------------------------------------------------------------ //

  async createCampaign(campaign) {
    const cid  = campaign.campaign_id ?? randomUUID();
    const item = {
      ...campaign,
      campaign_id: cid,
      created_at:  campaign.created_at ?? new Date().toISOString(),
      status:      campaign.status ?? 'active',
    };
    await this.ddb.send(new PutCommand({ TableName: this._table('campaigns'), Item: item }));
    return cid;
  }

  async getActiveCampaigns(nodeType = null) {
    const params = {
      TableName:                 this._table('campaigns'),
      FilterExpression:          '#st = :s',
      ExpressionAttributeNames:  { '#st': 'status' },
      ExpressionAttributeValues: { ':s': 'active' },
    };
    if (nodeType) {
      params.FilterExpression          += ' AND node_type = :nt';
      params.ExpressionAttributeValues[':nt'] = nodeType;
    }
    const resp = await this.ddb.send(new ScanCommand(params));
    return resp.Items ?? [];
  }

  async pauseCampaign(campaignId) {
    await this.ddb.send(new UpdateCommand({
      TableName:                 this._table('campaigns'),
      Key:                       { campaign_id: campaignId },
      UpdateExpression:          'SET #st = :s',
      ExpressionAttributeNames:  { '#st': 'status' },
      ExpressionAttributeValues: { ':s': 'paused' },
    }));
  }

  // ------------------------------------------------------------------ //
  //  CALL RECORDS                                                        //
  // ------------------------------------------------------------------ //

  async logCall(record) {
    const callId = record.call_id ?? randomUUID();
    const item   = {
      ...record,
      call_id:   callId,
      timestamp: record.timestamp ?? new Date().toISOString(),
    };
    await this.ddb.send(new PutCommand({ TableName: this._table('calls'), Item: item }));
    return callId;
  }

  async getCallStats(nodeId = null) {
    const params = { TableName: this._table('calls') };
    if (nodeId) {
      params.FilterExpression          = 'node_id = :n';
      params.ExpressionAttributeValues = { ':n': nodeId };
    }
    const resp  = await this.ddb.send(new ScanCommand(params));
    const items = resp.Items ?? [];
    return {
      total:        items.length,
      connected:    items.filter(i => i.status === 'connected').length,
      human_replies: items.filter(i => i.human_reply === true).length,
    };
  }

  // ------------------------------------------------------------------ //
  //  S3 CONTENT STORE                                                    //
  // ------------------------------------------------------------------ //

  async saveContent(key, content, bucket = CONTENT_BUCKET) {
    await this.s3.send(new PutObjectCommand({
      Bucket:      bucket,
      Key:         key,
      Body:        content,
      ContentType: 'text/plain',
    }));
    return `s3://${bucket}/${key}`;
  }

  async loadContent(key, bucket = CONTENT_BUCKET) {
    const resp   = await this.s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    const chunks = [];
    for await (const chunk of resp.Body) chunks.push(chunk);
    return Buffer.concat(chunks).toString('utf-8');
  }

  async listContent(prefix, bucket = CONTENT_BUCKET) {
    const resp = await this.s3.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix }));
    return (resp.Contents ?? []).map(obj => obj.Key);
  }
}
