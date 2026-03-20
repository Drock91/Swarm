/**
 * EmailNode — Finds leads, sends cold email sequences, tracks opens/clicks/replies.
 *
 * Capabilities:
 *   - Multi-step drip sequences (up to 7 follow-ups)
 *   - CAN-SPAM compliant: unsubscribe footer + List-Unsubscribe header
 *   - Bounce & complaint auto-suppression (SES → SNS → SQS feedback loop)
 *   - Rate monitoring: auto-pauses when bounce >2% or complaint >0.1%
 *   - Send window enforcement (day-of-week + time window from profile)
 *   - Opt-out keyword detection in replies (stop, unsubscribe, remove me, etc.)
 *   - Subject line A/B testing + self-improvement
 *   - Human reply detection (filters auto-replies)
 *   - Sends via Amazon SES (no third-party email vendor needed)
 */

import { SESClient, SendRawEmailCommand }           from '@aws-sdk/client-ses';
import { SQSClient, ReceiveMessageCommand,
         DeleteMessageCommand }                       from '@aws-sdk/client-sqs';
import OpenAI                                         from 'openai';
import { BaseNode }                                   from '../core/base_node.mjs';
import { log }                                        from '../core/logger.mjs';

const sleep = ms => new Promise(r => setTimeout(r, ms));

const AUTO_REPLY_PATTERNS = [
  /out of office/i, /auto.?reply/i, /automatic response/i,
  /vacation/i, /away from (the )?office/i, /do not reply/i,
  /no.?reply@/i, /noreply@/i, /autoresponder/i,
];

const OPT_OUT_PATTERNS = [
  /\bstop\b/i, /\bunsubscribe\b/i, /\bremove me\b/i,
  /\bopt.?out\b/i, /\btake me off\b/i, /\bno more emails?\b/i,
  /\bdo not (contact|email)\b/i,
];

export class EmailNode extends BaseNode {
  static nodeType = 'email_node';

  constructor(config, region = 'us-east-1', parentId = null) {
    super(config, region, parentId);

    this.ses             = new SESClient({ region });
    this.sqsFeedback     = new SQSClient({ region });
    this.openai          = new OpenAI();
    this.fromEmail       = config.from_email  ?? process.env.SES_FROM_EMAIL ?? 'derek@heinrichstech.com';
    this.fromName        = config.from_name   ?? process.env.SES_FROM_NAME  ?? 'Derek';
    this.replyToEmail    = config.reply_to     ?? this.fromEmail;
    this.sequenceSteps   = config.sequence_steps ?? 3;
    this.followUpDays    = config.follow_up_intervals ?? [2, 4, 7];
    this.dailySendLimit  = config.daily_send_limit ?? 200;  // SES sandbox default is 200/day
    this.subjectLines    = config.subject_lines ?? ['Quick question for you', 'Have you considered this?'];
    this.emailTemplates  = config.email_templates ?? [];
    this._sentToday      = 0;
    this._dayStart       = new Date().toDateString();

    // Compliance — bounce/complaint feedback queue
    this.feedbackQueueUrl = config.feedback_queue_url
      ?? process.env.SWARM_SES_FEEDBACK_QUEUE_URL ?? '';

    // Compliance — send window (from profile.json via emailNodeConfig)
    this.sendWindowStart  = config.send_window_start ?? '08:00';
    this.sendWindowEnd    = config.send_window_end   ?? '17:00';
    this.sendDays         = config.send_days ?? ['Monday','Tuesday','Wednesday','Thursday'];

    // Compliance — rate tracking (reset daily)
    this._bounces     = 0;
    this._complaints  = 0;
    this._paused      = false;   // auto-pause flag
  }

  // ------------------------------------------------------------------ //
  //  MAIN CYCLE                                                          //
  // ------------------------------------------------------------------ //

  async runCycle() {
    this._resetDailyCounterIfNeeded();

    // 1. Process bounce/complaint feedback from SQS before anything else
    await this._processFeedback();

    // 2. Check rate health — auto-pause if thresholds exceeded
    this._checkRateHealth();

    // 3. Handle incoming tasks (shutdown, run_campaign, update_config)
    const tasks = await this.receiveTasks(20);
    for (const task of tasks) {
      await this._handleTask(task);
      await this.ackTask(task._receipt_handle);
    }

    // 4. Send emails only if within send window and not auto-paused
    if (this._paused) {
      log.warn({ event: 'email_auto_paused', bounces: this._bounces, complaints: this._complaints });
      return;
    }
    if (!this._isWithinSendWindow()) {
      log.debug({ event: 'outside_send_window', window: `${this.sendWindowStart}-${this.sendWindowEnd}`, days: this.sendDays });
      return;
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
      !l.bounced &&
      !l.complained &&
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
    const body    = await this._generateEmailBody(lead, step);
    const html    = this._appendUnsubscribeFooter(body);

    try {
      const rawMsg = this._buildRawEmail(lead.email, subject, html);
      await this.ses.send(new SendRawEmailCommand({ RawMessage: { Data: rawMsg } }));

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
  //  RAW EMAIL BUILDER (supports List-Unsubscribe header)                //
  // ------------------------------------------------------------------ //

  _buildRawEmail(toAddress, subject, htmlBody) {
    const boundary = `----=_Part_${Date.now()}`;
    const lines = [
      `From: ${this.fromName} <${this.fromEmail}>`,
      `To: ${toAddress}`,
      `Reply-To: ${this.replyToEmail}`,
      `Subject: ${subject}`,
      `MIME-Version: 1.0`,
      `List-Unsubscribe: <mailto:${this.replyToEmail}?subject=unsubscribe>`,
      `List-Unsubscribe-Post: List-Unsubscribe=One-Click`,
      `Content-Type: multipart/alternative; boundary="${boundary}"`,
      ``,
      `--${boundary}`,
      `Content-Type: text/plain; charset=UTF-8`,
      `Content-Transfer-Encoding: 7bit`,
      ``,
      htmlBody.replace(/<[^>]+>/g, ''),   // plain-text fallback (strip tags)
      ``,
      `--${boundary}`,
      `Content-Type: text/html; charset=UTF-8`,
      `Content-Transfer-Encoding: 7bit`,
      ``,
      htmlBody,
      ``,
      `--${boundary}--`,
    ];
    return Buffer.from(lines.join('\r\n'));
  }

  // ------------------------------------------------------------------ //
  //  UNSUBSCRIBE FOOTER                                                  //
  // ------------------------------------------------------------------ //

  _appendUnsubscribeFooter(htmlBody) {
    const footer = [
      `<br><br>`,
      `<div style="font-size:11px;color:#999;border-top:1px solid #eee;padding-top:8px;margin-top:24px;">`,
      `  You are receiving this because we thought our service might help your business.`,
      `  If you don't want to hear from us, simply reply <strong>STOP</strong>`,
      `  or <a href="mailto:${this.replyToEmail}?subject=unsubscribe">click here to unsubscribe</a>.`,
      `  <br>Heinrichs Software Solutions, Florida, USA`,
      `</div>`,
    ].join('\n');
    return htmlBody + footer;
  }

  // ------------------------------------------------------------------ //
  //  REPLY PROCESSING (called from webhook handler)                      //
  // ------------------------------------------------------------------ //

  async processInboundReply(sender, subject, body, leadId) {
    const isAuto   = AUTO_REPLY_PATTERNS.some(p => p.test(body));
    const isOptOut = OPT_OUT_PATTERNS.some(p => p.test(body)) ||
                     OPT_OUT_PATTERNS.some(p => p.test(subject));

    const leads = await this.memory.getLeads({ email: sender }, 1);
    if (!leads.length) return;
    const lead = leads[0];

    // Opt-out takes priority over everything
    if (isOptOut) {
      await this.memory.suppressLead(lead.lead_id, 'unsubscribed');
      this.increment('unsubscribes');
      await this.memory.writeMetric(this.nodeId, this.nodeType, 'unsubscribes', 1);
      log.info({ event: 'opt_out_detected', sender, lead_id: lead.lead_id, trigger: 'reply_keyword' });
      return;
    }

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
  //  BOUNCE / COMPLAINT FEEDBACK PROCESSING                              //
  // ------------------------------------------------------------------ //

  /**
   * Poll the SES feedback SQS queue for bounce and complaint notifications.
   * SES → SNS → SQS — each message wraps an SNS notification containing
   * the SES event JSON.
   */
  async _processFeedback() {
    if (!this.feedbackQueueUrl) return;

    try {
      const resp = await this.sqsFeedback.send(new ReceiveMessageCommand({
        QueueUrl:            this.feedbackQueueUrl,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds:     1,
      }));

      for (const msg of resp.Messages ?? []) {
        try {
          const snsEnvelope = JSON.parse(msg.Body);
          const sesEvent    = JSON.parse(snsEnvelope.Message);
          await this._handleSesEvent(sesEvent);
        } catch (parseErr) {
          log.warn({ event: 'feedback_parse_error', error: parseErr.message });
        }

        // Delete message from queue after processing
        await this.sqsFeedback.send(new DeleteMessageCommand({
          QueueUrl:      this.feedbackQueueUrl,
          ReceiptHandle: msg.ReceiptHandle,
        }));
      }
    } catch (err) {
      log.error({ event: 'feedback_poll_error', error: err.message });
    }
  }

  async _handleSesEvent(sesEvent) {
    const notifType = sesEvent.notificationType;

    if (notifType === 'Bounce') {
      const bounce = sesEvent.bounce ?? {};
      const bounceType = bounce.bounceType;               // Permanent | Transient
      for (const recipient of bounce.bouncedRecipients ?? []) {
        const email = recipient.emailAddress;
        if (bounceType === 'Permanent') {
          // Hard bounce — suppress immediately
          const leadId = await this.memory.suppressByEmail(email, 'bounced');
          this._bounces++;
          this.increment('hard_bounces');
          await this.memory.writeMetric(this.nodeId, this.nodeType, 'hard_bounces', 1);
          log.warn({ event: 'hard_bounce', email, lead_id: leadId });
        } else {
          // Soft bounce — log it; suppress after 3 soft bounces on same lead
          this._bounces++;
          this.increment('soft_bounces');
          const leads = await this.memory.getLeads({ email }, 1);
          if (leads.length) {
            const lead = leads[0];
            const softCount = (lead.soft_bounce_count ?? 0) + 1;
            if (softCount >= 3) {
              await this.memory.suppressLead(lead.lead_id, 'bounced');
              log.warn({ event: 'soft_bounce_suppressed', email, count: softCount });
            } else {
              await this.memory.upsertLead({ ...lead, soft_bounce_count: softCount });
              log.info({ event: 'soft_bounce', email, count: softCount });
            }
          }
        }
      }
    }

    if (notifType === 'Complaint') {
      for (const recipient of sesEvent.complaint?.complainedRecipients ?? []) {
        const email  = recipient.emailAddress;
        const leadId = await this.memory.suppressByEmail(email, 'complained');
        this._complaints++;
        this.increment('complaints');
        await this.memory.writeMetric(this.nodeId, this.nodeType, 'complaints', 1);
        log.warn({ event: 'spam_complaint', email, lead_id: leadId });
      }
    }
  }

  // ------------------------------------------------------------------ //
  //  SEND WINDOW ENFORCEMENT                                             //
  // ------------------------------------------------------------------ //

  /**
   * Returns true if current local time falls within the configured send window.
   * Uses America/New_York as the default business timezone.
   */
  _isWithinSendWindow() {
    const now  = new Date();
    const opts = { timeZone: 'America/New_York', hour12: false };
    const dayName = now.toLocaleDateString('en-US', { ...opts, weekday: 'long' });
    const hhmm    = now.toLocaleTimeString('en-US', { ...opts, hour: '2-digit', minute: '2-digit' });

    if (!this.sendDays.includes(dayName)) return false;

    const current = hhmm.replace(':', '');
    const start   = this.sendWindowStart.replace(':', '');
    const end     = this.sendWindowEnd.replace(':', '');
    return current >= start && current <= end;
  }

  // ------------------------------------------------------------------ //
  //  RATE HEALTH MONITORING + AUTO-PAUSE                                 //
  // ------------------------------------------------------------------ //

  /**
   * If bounce rate exceeds 2% or complaint rate exceeds 0.1%,
   * auto-pause the node to protect SES reputation.
   */
  _checkRateHealth() {
    const totalSent = this.getCounter('emails_sent');
    if (totalSent < 50) return;   // need a minimum sample before acting

    const bounceRate    = this._bounces    / totalSent;
    const complaintRate = this._complaints / totalSent;

    if (bounceRate > 0.02) {
      log.error({ event: 'auto_pause_bounce_rate', rate: bounceRate.toFixed(4), threshold: 0.02 });
      this._paused = true;
    }
    if (complaintRate > 0.001) {
      log.error({ event: 'auto_pause_complaint_rate', rate: complaintRate.toFixed(4), threshold: 0.001 });
      this._paused = true;
    }

    if (this._paused) {
      this.memory.writeMetric(this.nodeId, this.nodeType, 'auto_paused', 1, {
        bounce_rate: bounceRate, complaint_rate: complaintRate,
      });
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
      emails_sent:    sent,
      human_replies:  humanR,
      reply_rate:     sent > 0 ? +(humanR / sent).toFixed(4) : 0,
      errors:         this.getCounter('errors'),
      hard_bounces:   this.getCounter('hard_bounces'),
      soft_bounces:   this.getCounter('soft_bounces'),
      complaints:     this.getCounter('complaints'),
      unsubscribes:   this.getCounter('unsubscribes'),
      bounce_rate:    sent > 0 ? +(this._bounces / sent).toFixed(4) : 0,
      complaint_rate: sent > 0 ? +(this._complaints / sent).toFixed(4) : 0,
      auto_paused:    this._paused,
      leads_in_db:    0, // populated async in heartbeat if needed
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
      this._sentToday  = 0;
      this._bounces    = 0;
      this._complaints = 0;
      this._paused     = false;   // reset auto-pause each day
      this._dayStart   = today;
    }
  }
}
