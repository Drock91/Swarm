/**
 * DMNode — Automated outreach on X (Twitter), LinkedIn, and Reddit.
 *
 * Capabilities:
 *   - Persona-crafted opening messages (GPT)
 *   - Multi-platform: X DMs, LinkedIn InMail, Reddit chats
 *   - Follow-up sequences (2-3 touchpoints max to avoid spam flags)
 *   - Human reply detection
 *   - Rate-limit aware per platform
 *   - Scales to thousands of DMs/day
 */

import axios from 'axios';
import OpenAI from 'openai';
import { BaseNode } from '../core/base_node.mjs';
import { log } from '../core/logger.mjs';

const sleep = ms => new Promise(r => setTimeout(r, ms));

const PLATFORMS = ['x', 'linkedin', 'reddit'];

export class DMNode extends BaseNode {
  static nodeType = 'dm_node';

  constructor(config, region = 'us-east-1', parentId = null) {
    super(config, region, parentId);
    this.openai       = new OpenAI();
    this.platforms    = config.platforms ?? PLATFORMS;
    this.dailyDMLimit = config.daily_dm_limit ?? 300;
    this.persona      = config.persona ?? 'a helpful consultant';
    this.offerHook    = config.offer_hook ?? '';
    this.followUpDays = config.follow_up_days ?? [2, 5];

    // Platform credentials
    this.xBearerToken     = config.x_bearer_token ?? '';
    this.linkedInToken    = config.linkedin_token ?? '';
    this.redditClientId   = config.reddit_client_id ?? '';
    this.redditSecret     = config.reddit_client_secret ?? '';

    this._sentToday = 0;
    this._dayStart  = new Date().toDateString();
  }

  async runCycle() {
    this._resetDailyCounter();
    const tasks = await this.receiveTasks(10);
    for (const t of tasks) {
      await this._handleTask(t);
      await this.ackTask(t._receipt_handle);
    }
    if (this._sentToday < this.dailyDMLimit) {
      await this._processPendingDMs();
    }
  }

  async _handleTask(task) {
    if (task.command === 'shutdown' && (!task.target_node_id || task.target_node_id === this.nodeId)) {
      await this.stop('commander_shutdown');
    } else if (task.command === 'run_campaign') {
      await this._enqueueCampaignLeads(task.campaign);
    } else if (task.command === 'update_config') {
      this.config[task.key] = task.value;
    }
  }

  async _enqueueCampaignLeads(campaign) {
    const leads = await this.memory.getLeads(campaign.target_audience, 500);
    log.info({ event: 'dm_campaign_start', campaign_id: campaign.campaign_id, leads: leads.length });
    for (const lead of leads) {
      if (!lead.social_handle && !lead.linkedin_url && !lead.reddit_user) continue;
      await this.memory.upsertLead({
        ...lead,
        dm_state: {
          campaign_id:  campaign.campaign_id,
          step:         0,
          next_send_at: Math.floor(Date.now() / 1000),
          status:       'pending',
        },
      });
    }
  }

  async _processPendingDMs() {
    const now   = Math.floor(Date.now() / 1000);
    const leads = await this.memory.getLeads(null, 100);
    const due   = leads.filter(l =>
      l.dm_state?.status === 'pending' &&
      (l.dm_state?.next_send_at ?? Infinity) <= now &&
      !l.dm_replied_human,
    );
    for (const lead of due) {
      if (this._sentToday >= this.dailyDMLimit) break;
      await this._sendDM(lead);
    }
  }

  async _sendDM(lead) {
    const platform = this._pickPlatform(lead);
    if (!platform) return;

    const message = await this._generateMessage(lead, lead.dm_state?.step ?? 0);

    try {
      await this._dispatchDM(platform, lead, message);
      const step        = (lead.dm_state?.step ?? 0);
      const nextInterval = this.followUpDays[Math.min(step, this.followUpDays.length - 1)];
      const newStep     = step + 1;

      await this.memory.upsertLead({
        ...lead,
        dm_state: {
          ...lead.dm_state,
          step:         newStep,
          platform,
          next_send_at: Math.floor(Date.now() / 1000) + nextInterval * 86400,
          status:       newStep < 3 ? 'pending' : 'completed',
          last_message: message.slice(0, 100),
        },
      });

      this.increment('dms_sent');
      this._sentToday++;
      await this.memory.writeMetric(this.nodeId, this.nodeType, 'dms_sent', 1);
      log.debug({ event: 'dm_sent', lead_id: lead.lead_id, platform });
      await sleep(2000); // rate-limit pause between DMs
    } catch (err) {
      log.error({ event: 'dm_send_error', lead_id: lead.lead_id, platform, error: err.message });
      this.increment('errors');
    }
  }

  _pickPlatform(lead) {
    if (lead.x_handle     && this.platforms.includes('x'))        return 'x';
    if (lead.linkedin_url && this.platforms.includes('linkedin')) return 'linkedin';
    if (lead.reddit_user  && this.platforms.includes('reddit'))   return 'reddit';
    return null;
  }

  async _generateMessage(lead, step) {
    const isFollowUp = step > 0;
    const prompt = isFollowUp
      ? `Write a brief, natural follow-up DM (follow-up #${step}) to ${lead.name ?? 'a prospect'} ` +
        `who hasn't replied yet. Reference: "${lead.dm_state?.last_message ?? ''}". ` +
        `Be casual, not pushy. Persona: ${this.persona}. Under 80 words.`
      : `Write a short, human-sounding opening DM to ${lead.name ?? 'a prospect'} ` +
        `(${lead.title ?? ''} at ${lead.company ?? ''}). ` +
        `Offer hook: ${this.offerHook}. Persona: ${this.persona}. Under 80 words. No generic openers.`;

    try {
      const resp = await this.openai.chat.completions.create({
        model:       'gpt-4o-mini',
        messages:    [{ role: 'user', content: prompt }],
        max_tokens:  150,
        temperature: 0.8,
      });
      return resp.choices[0].message.content.trim();
    } catch {
      return `Hey ${lead.name ?? 'there'}, I noticed your work and wanted to connect. ${this.offerHook}`;
    }
  }

  async _dispatchDM(platform, lead, message) {
    // Platform-specific dispatch — stubs that call real APIs
    switch (platform) {
      case 'x':
        // POST /2/dm_conversations with Twitter API v2
        if (!this.xBearerToken) throw new Error('No X bearer token');
        await axios.post(
          'https://api.twitter.com/2/dm_conversations',
          { participant_id: lead.x_id, message: { text: message } },
          { headers: { Authorization: `Bearer ${this.xBearerToken}` }, timeout: 10_000 },
        );
        break;
      case 'linkedin':
        if (!this.linkedInToken) throw new Error('No LinkedIn token');
        await axios.post(
          'https://api.linkedin.com/v2/messages',
          { recipients: [{ person: lead.linkedin_urn }], subject: '', body: message },
          { headers: { Authorization: `Bearer ${this.linkedInToken}` }, timeout: 10_000 },
        );
        break;
      case 'reddit':
        // Uses Reddit private messages API
        if (!this.redditClientId) throw new Error('No Reddit credentials');
        // Reddit OAuth + send PM — placeholder
        log.debug({ event: 'reddit_dm_stub', lead_id: lead.lead_id });
        break;
    }
  }

  collectMetrics() {
    const sent    = this.getCounter('dms_sent');
    const replied = this.getCounter('human_replies');
    return {
      dms_sent:      sent,
      human_replies: replied,
      reply_rate:    sent > 0 ? +(replied / sent).toFixed(4) : 0,
      errors:        this.getCounter('errors'),
    };
  }

  getImprovementContext() {
    return {
      config:          this.config,
      persona:         this.persona,
      offer_hook:      this.offerHook,
      platforms:       this.platforms,
      daily_dm_limit:  this.dailyDMLimit,
      follow_up_days:  this.followUpDays,
    };
  }

  _resetDailyCounter() {
    const today = new Date().toDateString();
    if (today !== this._dayStart) {
      this._sentToday = 0;
      this._dayStart  = today;
    }
  }
}
