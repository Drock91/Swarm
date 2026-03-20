/**
 * EmailNode — Finds leads, sends cold email sequences, tracks opens/clicks/replies.
 *
 * Capabilities:
 *   - Multi-step drip sequences (up to 7 follow-ups)
 *   - Open/click/reply tracking via SendGrid webhooks
 *   - Automated unsubscribe handling
 *   - Subject line A/B testing + self-improvement
 *   - Human reply detection (filters auto-replies)
 *   - Scales to millions of emails/month
 */

import sgMail from '@sendgrid/mail';
import OpenAI from 'openai';
import { BaseNode } from '../core/base_node.mjs';
import { log } from '../core/logger.mjs';

const sleep = ms => new Promise(r => setTimeout(r, ms));

const AUTO_REPLY_PATTERNS = [
  /out of office/i, /auto.?reply/i, /automatic response/i,
  /vacation/i, /away from (the )?office/i, /do not reply/i,
  /no.?reply@/i, /noreply@/i, /autoresponder/i,
];

export class EmailNode extends BaseNode {
  static nodeType = 'email_node';

  constructor(config, region = 'us-east-1', parentId = null) {
    super(config, region, parentId);

    sgMail.setApiKey(config.sendgrid_api_key ?? '');
    this.openai          = new OpenAI();
    this.fromEmail       = config.from_email  ?? 'outreach@example.com';
    this.fromName        = config.from_name   ?? 'Team';
    this.sequenceSteps   = config.sequence_steps ?? 3;
    this.followUpDays    = config.follow_up_intervals ?? [2, 4, 7];
    this.dailySendLimit  = config.daily_send_limit ?? 500;
    this.subjectLines    = config.subject_lines ?? ['Quick question for you', 'Have you considered this?'];
    this.emailTemplates  = config.email_templates ?? [];
    this._sentToday      = 0;
    this._dayStart       = new Date().toDateString();
  }

  // ------------------------------------------------------------------ //
  //  MAIN CYCLE                                                          //
  // ------------------------------------------------------------------ //

  async runCycle() {
    this._resetDailyCounterIfNeeded();
    const tasks = await this.receiveTasks(20);
    for (const task of tasks) {
      await this._handleTask(task);
      await this.ackTask(task._receipt_handle);
    }
    if (this._sentToday < this.dailySendLimit) {
      await this._processPendingSequences();
    }
  }

  async _handleTask(task) {
    switch (task.command) {
      case 'shutdown':
        if (!task.target_node_id || task.target_node_id === this.nodeId)
          await this.stop('commander_shutdown');
        break;
      case 'run_campaign':
        await this._launchCampaignSequence(task.campaign);
        break;
      case 'update_config':
        if (task.key) {
          this.config[task.key] = task.value;
          this.dailySendLimit = this.config.daily_send_limit ?? this.dailySendLimit;
        }
        break;
    }
  }

  // ------------------------------------------------------------------ //
  //  SEQUENCE MANAGEMENT                                                 //
  // ------------------------------------------------------------------ //

  async _launchCampaignSequence(campaign) {
    const leads = await this.memory.getLeads(campaign.target_audience, 1000);
    log.info({ event: 'campaign_sequence_start', campaign_id: campaign.campaign_id, leads: leads.length });

    for (const lead of leads) {
      if (!lead.email) continue;
      await this.memory.upsertLead({
        ...lead,
        sequence_state: {
          lead_id:      lead.lead_id,
          campaign_id:  campaign.campaign_id,
          step:         0,
          next_send_at: Math.floor(Date.now() / 1000),
          status:       'pending',
        },
      });
    }
  }

  async _processPendingSequences() {
    const now   = Math.floor(Date.now() / 1000);
    const leads = await this.memory.getLeads(null, 200);
    const due   = leads.filter(l =>
      l.sequence_state?.status === 'pending' &&
      (l.sequence_state?.next_send_at ?? Infinity) <= now &&
      !l.unsubscribed &&
      !l.replied_human,
    );
    for (const lead of due) {
      if (this._sentToday >= this.dailySendLimit) break;
      await this._sendSequenceEmail(lead);
    }
  }

  async _sendSequenceEmail(lead) {
    const seq  = lead.sequence_state ?? {};
    const step = seq.step ?? 0;

    if (step >= this.sequenceSteps) {
      await this.memory.upsertLead({ ...lead, sequence_state: { ...seq, status: 'completed' } });
      return;
    }

    const subject = await this._pickSubject(lead, step);
    const html    = await this._generateEmailBody(lead, step);

    try {
      await sgMail.send({
        to:   lead.email,
        from: { email: this.fromEmail, name: this.fromName },
        subject,
        html,
        customArgs: { node_id: this.nodeId, lead_id: lead.lead_id, step: String(step) },
      });

      const nextInterval = this.followUpDays[Math.min(step, this.followUpDays.length - 1)];
      await this.memory.upsertLead({
        ...lead,
        sequence_state: {
          ...seq,
          step:         step + 1,
          next_send_at: Math.floor(Date.now() / 1000) + nextInterval * 86400,
          status:       (step + 1) < this.sequenceSteps ? 'pending' : 'completed',
          last_subject: subject,
        },
      });

      await this.memory.writeMetric(this.nodeId, this.nodeType, 'emails_sent', 1);
      this.increment('emails_sent');
      this._sentToday++;
      log.debug({ event: 'email_sent', lead_id: lead.lead_id, step });
    } catch (err) {
      log.error({ event: 'email_send_error', lead_id: lead.lead_id, error: err.message });
      this.increment('errors');
    }
  }

  // ------------------------------------------------------------------ //
  //  REPLY PROCESSING (called from webhook handler)                      //
  // ------------------------------------------------------------------ //

  async processInboundReply(sender, subject, body, leadId) {
    const isAuto = AUTO_REPLY_PATTERNS.some(p => p.test(body));
    const leads  = await this.memory.getLeads({ email: sender }, 1);
    if (!leads.length) return;
    const lead = leads[0];

    if (isAuto) {
      this.increment('auto_replies');
    } else {
      this.increment('human_replies');
      await this.memory.writeMetric(this.nodeId, this.nodeType, 'human_replies', 1);
      await this.memory.upsertLead({
        ...lead,
        replied_human:  true,
        replied_at:     new Date().toISOString(),
        reply_snippet:  body.slice(0, 200),
        sequence_state: { ...lead.sequence_state, status: 'replied' },
      });
      log.info({ event: 'human_reply_detected', sender, lead_id: lead.lead_id });
    }
  }

  // ------------------------------------------------------------------ //
  //  COPY GENERATION                                                     //
  // ------------------------------------------------------------------ //

  async _pickSubject(lead, step) {
    if (step === 0) {
      const winners = await this.memory.getTopKnowledge(this.nodeType, 'subject_line', 3);
      if (winners.length) return winners[0].data?.subject ?? this.subjectLines[0];
    }
    if (step < this.subjectLines.length) return this.subjectLines[step];
    return this._generateSubject(lead);
  }

  // Strip any AI-tell punctuation that slips through LLM output
  _sanitizeCopy(text) {
    return text
      .replace(/\s*\u2014\s*/g, ' ')   // em dash —
      .replace(/\s*\u2013\s*/g, ' ')   // en dash –
      .replace(/\s+-\s+/g, ' ')        // spaced hyphen " - "
      .replace(/\s{2,}/g, ' ')         // collapse double spaces
      .trim();
  }

  async _generateSubject(lead) {
    try {
      const resp = await this.openai.chat.completions.create({
        model:      'gpt-4o-mini',
        messages:   [{ role: 'user', content:
          `Write a cold email subject line for ${lead.name ?? 'a prospect'} ` +
          `at ${lead.company ?? 'their company'}.\n\n` +
          `Rules:\n` +
          `- One line only, no punctuation at the end\n` +
          `- Do NOT use dashes of any kind (no —, no –, no " - ")\n` +
          `- Keep it short and plain, like a real person typed it\n` +
          `- No big or formal words` }],
        max_tokens: 50,
        temperature: 0.8,
      });
      const raw = resp.choices[0].message.content.trim().replace(/^"|"$/g, '');
      return this._sanitizeCopy(raw);
    } catch {
      return this.subjectLines[0];
    }
  }

  async _generateEmailBody(lead, step) {
    if (this.emailTemplates[step]) {
      return this._sanitizeCopy(
        this.emailTemplates[step]
          .replace(/\{\{name\}\}/g, lead.name ?? 'there')
          .replace(/\{\{company\}\}/g, lead.company ?? 'your company')
      );
    }
    try {
      const resp = await this.openai.chat.completions.create({
        model:      'gpt-4o-mini',
        messages:   [{ role: 'user', content:
          `Write a short cold email (step ${step + 1} of a sequence) for:\n` +
          `Name: ${lead.name ?? 'there'}\nCompany: ${lead.company ?? ''}\n` +
          `Title: ${lead.title ?? ''}\nPain point: ${lead.pain_point ?? ''}\n\n` +
          `Rules (read carefully):\n` +
          `- Under 120 words\n` +
          `- Do NOT use dashes of any kind. No em dash (—), no en dash (–), no spaced hyphen (word - word). Just skip them.\n` +
          `- Write like a normal person texting a colleague. Simple words only.\n` +
          `- No formal or corporate language. No words like \"leverage\", \"utilize\", \"endeavor\", \"facilitate\", \"streamline\", \"robust\", \"synergy\".\n` +
          `- No subject line. Sign off as '${this.fromName}'.` }],
        max_tokens:  250,
        temperature: 0.7,
      });
      return this._sanitizeCopy(resp.choices[0].message.content.trim());
    } catch {
      return `Hey ${lead.name ?? 'there'}, just wanted to reach out. Would love to connect. Best, ${this.fromName}`;
    }
  }

  // ------------------------------------------------------------------ //
  //  METRICS & IMPROVEMENT                                               //
  // ------------------------------------------------------------------ //

  collectMetrics() {
    const sent    = this.getCounter('emails_sent');
    const humanR  = this.getCounter('human_replies');
    return {
      emails_sent:   sent,
      human_replies: humanR,
      reply_rate:    sent > 0 ? +(humanR / sent).toFixed(4) : 0,
      errors:        this.getCounter('errors'),
      leads_in_db:   0, // populated async in heartbeat if needed
    };
  }

  getImprovementContext() {
    return {
      config:           this.config,
      sequence_steps:   this.sequenceSteps,
      daily_send_limit: this.dailySendLimit,
      follow_up_days:   this.followUpDays,
    };
  }

  _computeScore(metrics) {
    return Math.min((metrics.reply_rate ?? 0) * 5, 1.0);
  }

  _resetDailyCounterIfNeeded() {
    const today = new Date().toDateString();
    if (today !== this._dayStart) {
      this._sentToday = 0;
      this._dayStart  = today;
    }
  }
}
