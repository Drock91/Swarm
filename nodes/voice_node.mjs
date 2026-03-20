/**
 * VoiceNode — AI cold calls via Twilio + ElevenLabs.
 *
 * Capabilities:
 *   - Outbound calls triggered via Twilio Programmable Voice
 *   - ElevenLabs voice synthesis for dynamic, natural-sounding AI caller
 *   - Call scripts generated and improved by GPT
 *   - Human-answer detection (distinguishes voicemail vs live person)
 *   - Human reply tracking (non-scripted responses back to us)
 *   - Stores all call records in SharedMemory (swarm-calls table)
 *   - Rate-limited to comply with TCPA guidelines
 *   - Scales to hundreds of calls/day
 */

import twilio from 'twilio';
import { ElevenLabsClient } from '@elevenlabs/elevenlabs-js';
import OpenAI from 'openai';
import { BaseNode } from '../core/base_node.mjs';
import { log } from '../core/logger.mjs';

const sleep = ms => new Promise(r => setTimeout(r, ms));

export class VoiceNode extends BaseNode {
  static nodeType = 'voice_node';

  constructor(config, region = 'us-east-1', parentId = null) {
    super(config, region, parentId);

    this.openai       = new OpenAI();
    this.twilioClient = twilio(config.twilio_account_sid ?? '', config.twilio_auth_token ?? '');
    this.elevenLabs   = new ElevenLabsClient({ apiKey: config.elevenlabs_api_key ?? '' });

    this.fromNumber      = config.twilio_from_number ?? '';
    this.voiceId         = config.elevenlabs_voice_id ?? 'EXAVITQu4vr4xnSDxMaL'; // default: Bella
    this.callScript      = config.call_script ?? '';
    this.dailyCallLimit  = config.daily_call_limit ?? 200;
    this.callWindowStart = config.call_window_start ?? 9;  // 9 AM local
    this.callWindowEnd   = config.call_window_end   ?? 17; // 5 PM local
    this.webhookBaseUrl  = config.webhook_base_url  ?? '';

    this._callsToday = 0;
    this._dayStart   = new Date().toDateString();
  }

  async runCycle() {
    this._resetDailyCounter();
    const tasks = await this.receiveTasks(10);
    for (const t of tasks) {
      await this._handleTask(t);
      await this.ackTask(t._receipt_handle);
    }
    if (this._callsToday < this.dailyCallLimit && this._inCallWindow()) {
      await this._processCallQueue();
    }
  }

  async _handleTask(task) {
    if (task.command === 'shutdown' && (!task.target_node_id || task.target_node_id === this.nodeId)) {
      await this.stop('commander_shutdown');
    } else if (task.command === 'run_campaign') {
      await this._enqueueCampaignLeads(task.campaign);
    } else if (task.command === 'update_config') {
      this.config[task.key] = task.value;
    } else if (task.command === 'call_result') {
      await this._processCallResult(task);
    }
  }

  async _enqueueCampaignLeads(campaign) {
    const leads = await this.memory.getLeads(campaign.target_audience, 300);
    log.info({ event: 'voice_campaign_start', campaign_id: campaign.campaign_id, leads: leads.length });
    for (const lead of leads) {
      if (!lead.phone) continue;
      await this.memory.upsertLead({
        ...lead,
        voice_state: {
          campaign_id:  campaign.campaign_id,
          status:       'queued',
          attempts:     0,
        },
      });
    }
  }

  async _processCallQueue() {
    const leads = await this.memory.getLeads(null, 50);
    const queued = leads.filter(l =>
      l.voice_state?.status === 'queued' &&
      (l.voice_state?.attempts ?? 0) < 2 &&
      !l.do_not_call &&
      l.phone,
    );

    for (const lead of queued) {
      if (this._callsToday >= this.dailyCallLimit) break;
      await this._placeCall(lead);
      await sleep(30_000); // 30-second gap between calls
    }
  }

  async _placeCall(lead) {
    const script = await this._generateCallScript(lead);

    // Generate ElevenLabs TTS audio and upload to S3 for Twilio to play
    let audioUrl = '';
    try {
      const audio = await this.elevenLabs.textToSpeech.convert(this.voiceId, {
        text:          script,
        model_id:      'eleven_turbo_v2',
        voice_settings: { stability: 0.5, similarity_boost: 0.8 },
      });
      const chunks = [];
      for await (const chunk of audio) chunks.push(chunk);
      const buffer = Buffer.concat(chunks);
      const s3Key  = `voice/${this.nodeId}/${lead.lead_id}-${Date.now()}.mp3`;
      audioUrl     = await this.memory.saveContent(s3Key, buffer.toString('base64'));
      audioUrl     = `${this.config.s3_public_base ?? ''}/${s3Key}`;
    } catch (err) {
      log.warn({ event: 'tts_error', lead_id: lead.lead_id, error: err.message });
    }

    // Place call via Twilio — plays TTS audio and handles keypress responses
    const twimlUrl = audioUrl
      ? `${this.webhookBaseUrl}/voice/twiml?audio=${encodeURIComponent(audioUrl)}&lead_id=${lead.lead_id}&node_id=${this.nodeId}`
      : `${this.webhookBaseUrl}/voice/twiml?lead_id=${lead.lead_id}&node_id=${this.nodeId}`;

    try {
      const call = await this.twilioClient.calls.create({
        to:                 lead.phone,
        from:               this.fromNumber,
        url:                twimlUrl,
        statusCallbackUrl:  `${this.webhookBaseUrl}/voice/status?lead_id=${lead.lead_id}&node_id=${this.nodeId}`,
        statusCallbackMethod: 'POST',
        machineDetection:   'Enable',
      });

      await this.memory.logCall({
        call_id:     call.sid,
        node_id:     this.nodeId,
        lead_id:     lead.lead_id,
        phone:       lead.phone,
        status:      'placed',
        twilio_sid:  call.sid,
        script_used: script.slice(0, 200),
      });

      await this.memory.upsertLead({
        ...lead,
        voice_state: {
          ...lead.voice_state,
          status:    'called',
          last_call: new Date().toISOString(),
          attempts:  (lead.voice_state?.attempts ?? 0) + 1,
        },
      });

      this.increment('calls_made');
      this._callsToday++;
      await this.memory.writeMetric(this.nodeId, this.nodeType, 'calls_made', 1);
      log.info({ event: 'call_placed', lead_id: lead.lead_id, twilio_sid: call.sid });
    } catch (err) {
      log.error({ event: 'call_error', lead_id: lead.lead_id, error: err.message });
      this.increment('errors');
    }
  }

  /** Called from webhook when Twilio posts back call status */
  async processCallStatus(callSid, status, answeredBy, leadId) {
    const isHuman = answeredBy === 'human';
    if (isHuman && status === 'completed') {
      this.increment('calls_connected');
      await this.memory.writeMetric(this.nodeId, this.nodeType, 'calls_connected', 1);
    }
    log.info({ event: 'call_status_update', call_sid: callSid, status, answered_by: answeredBy });
  }

  /** Called from webhook when a human responds (keypress or speech) */
  async processHumanReply(callSid, response, leadId) {
    this.increment('human_replies');
    await this.memory.writeMetric(this.nodeId, this.nodeType, 'human_replies', 1);
    // Update call record
    const leads = await this.memory.getLeads({ lead_id: leadId }, 1);
    if (leads.length) {
      await this.memory.upsertLead({ ...leads[0], voice_replied: true, voice_reply: response });
    }
    log.info({ event: 'voice_human_reply', call_sid: callSid, response });
  }

  async _generateCallScript(lead) {
    if (this.callScript) {
      return this.callScript
        .replace(/\{\{name\}\}/g, lead.name ?? 'there')
        .replace(/\{\{company\}\}/g, lead.company ?? 'your company');
    }
    try {
      const resp = await this.openai.chat.completions.create({
        model:      'gpt-4o-mini',
        messages:   [{ role: 'user', content:
          `Write a natural AI cold call script (30 seconds max) for:\n` +
          `Name: ${lead.name ?? 'a prospect'}\nCompany: ${lead.company ?? ''}\n` +
          `Title: ${lead.title ?? ''}\n` +
          `Offer: ${this.config.offer ?? 'a quick intro call'}.\n` +
          `Be warm, direct, and end with a clear yes/no question. No long pauses.` }],
        max_tokens:  200,
        temperature: 0.6,
      });
      return resp.choices[0].message.content.trim();
    } catch {
      return `Hi ${lead.name ?? 'there'}, this is a quick call about ${this.config.offer ?? 'something important'}. Can I take 30 seconds of your time?`;
    }
  }

  _inCallWindow() {
    const hour = new Date().getHours();
    return hour >= this.callWindowStart && hour < this.callWindowEnd;
  }

  collectMetrics() {
    const made      = this.getCounter('calls_made');
    const connected = this.getCounter('calls_connected');
    const replies   = this.getCounter('human_replies');
    return {
      calls_made:       made,
      calls_connected:  connected,
      connection_rate:  made > 0 ? +(connected / made).toFixed(4) : 0,
      human_replies:    replies,
      errors:           this.getCounter('errors'),
    };
  }

  getImprovementContext() {
    return {
      config:            this.config,
      call_script:       this.callScript.slice(0, 300),
      daily_call_limit:  this.dailyCallLimit,
      call_window:       [this.callWindowStart, this.callWindowEnd],
    };
  }

  _resetDailyCounter() {
    const today = new Date().toDateString();
    if (today !== this._dayStart) {
      this._callsToday = 0;
      this._dayStart   = today;
    }
  }
}
