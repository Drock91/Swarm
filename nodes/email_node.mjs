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

import nodemailer                                     from 'nodemailer';
import { SQSClient, ReceiveMessageCommand,
         DeleteMessageCommand }                       from '@aws-sdk/client-sqs';
import { chat }                                        from '../core/llm.mjs';
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

    this.transporter     = nodemailer.createTransport({
      host:   process.env.BREVO_SMTP_HOST ?? 'smtp-relay.brevo.com',
      port:   Number(process.env.BREVO_SMTP_PORT ?? 587),
      secure: false,
      auth: {
        user: process.env.BREVO_SMTP_USER,
        pass: process.env.BREVO_SMTP_PASS,
      },
    });
    this.sqsFeedback     = new SQSClient({ region });
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
    const tasks = await this.receiveTasks(10);
    for (const task of tasks) {
      await this._handleTask(task);
      await this.ackTask(task._receipt_handle);
    }

    // 4. Auto-seed any fresh leads from the scraper that have no sequence_state yet
    await this._seedNewLeads();

    // 5. Send emails only if within send window and not auto-paused
    if (this._paused) {
      log.warn({ event: 'email_auto_paused', bounces: this._bounces, complaints: this._complaints });
      return;
    }
    if (!this._isWithinSendWindow()) {
      log.info({ event: 'outside_send_window', window: `${this.sendWindowStart}-${this.sendWindowEnd}`, days: this.sendDays });
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

  /**
   * Pick up any leads the scraper stored that have no sequence_state yet
   * and stamp them as pending so they enter the send queue automatically.
   */
  async _seedNewLeads() {
    try {
      const all      = await this.memory.getLeads(null, 5000);
      const unseeded = all.filter(l =>
        l.email &&
        !l.sequence_state &&
        !l.unsubscribed &&
        !l.bounced &&
        !l.complained &&
        !l.suppressed,
      );
      if (!unseeded.length) return;
      const now = Math.floor(Date.now() / 1000);
      for (const lead of unseeded) {
        await this.memory.upsertLead({
          ...lead,
          sequence_state: {
            lead_id:      lead.lead_id,
            step:         0,
            next_send_at: now,
            status:       'pending',
          },
        });
      }
      log.info({ event: 'leads_seeded', count: unseeded.length });
    } catch (err) {
      log.error({ event: 'seed_leads_error', error: err.message });
    }
  }

  async _processPendingSequences() {
    const now   = Math.floor(Date.now() / 1000);
    const leads = await this.memory.getLeads(null, 2000);
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
      await this.transporter.sendMail({
        from:    `"${this.fromName}" <${this.fromEmail}>`,
        to:      lead.email,
        replyTo: this.replyToEmail,
        subject,
        html,
        text:    html.replace(/<[^>]+>/g, ''),
        headers: {
          'List-Unsubscribe':      `<mailto:${this.replyToEmail}?subject=unsubscribe>`,
          'List-Unsubscribe-Post': 'List-Unsubscribe=One-Click',
        },
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
      log.info({
        event:   'email_sent',
        step:    step + 1,
        to:      lead.email,
        company: lead.company ?? lead.website ?? '?',
        subject,
        body:    body.replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim().slice(0, 300),
      });
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
      if (winners.length) return winners[0].data?.subject ?? await this._generateSubject(lead);
    }
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

  _buildSenderContext() {
    const c = this.config;
    return [
      `You are writing on behalf of: ${c.from_name ?? this.fromName}`,
      `Business: ${c.business_name ?? 'Heinrichs Software Solutions'}`,
      `Website: ${c.business_website ?? 'heinrichstech.com'}`,
      `What we sell: ${c.offer_name ?? 'AI Chatbot'} at ${c.offer_price ?? '$49/month'}`,
      `Free trial: ${c.offer_trial ?? '14-day free trial, no credit card'}`,
      `What it solves: ${c.pain_solved ?? 'losing leads after hours because no one answers customer questions'}`,
      ...(c.differentiators?.length
        ? [`Key facts about us: ${c.differentiators.join(', ')}`]
        : []),
      `Sender persona: ${c.persona ?? 'direct, no-nonsense veteran founder'}`,
      ...(c.value_anchor ? [`Value comparison you can use: ${c.value_anchor}`] : []),
      `Sign off with:\n${c.signature ?? c.from_name ?? this.fromName}`,
    ].join('\n');
  }

  _buildSystemPrompt() {
    const c = this.config;
    return [
      `You are a cold email copywriter writing on behalf of ${c.from_name ?? this.fromName}.`,
      ``,
      `WHAT YOU ARE SELLING:`,
      `Product: ${c.offer_name ?? 'AI Chatbot'} — custom-built and managed for small businesses`,
      `Price: ${c.offer_price ?? '$79/month'}`,
      `Trial: ${c.offer_trial ?? '14-day free trial, no credit card required'}`,
      `Website: ${c.business_website ?? 'heinrichstech.com'}`,
      `What it solves: ${c.pain_solved ?? 'losing customers after hours because no one answers questions'}`,
      ...(c.value_anchor ? [`Value: ${c.value_anchor}`] : []),
      ``,
      `KEY FACTS ABOUT THE SENDER:`,
      ...(c.differentiators?.map(d => `- ${d}`) ?? []),
      ``,
      `SIGN OFF EVERY EMAIL WITH EXACTLY THIS AND NOTHING AFTER:`,
      c.signature ?? c.from_name ?? this.fromName,
      ``,
      `ABSOLUTE RULES — NEVER BREAK THESE:`,
      `- Every email MUST mention the chatbot product by name`,
      `- Every email MUST mention the free trial`,
      `- Every email MUST include the website: ${c.business_website ?? 'heinrichstech.com'}`,
      `- Under 100 words`,
      `- No dashes of any kind (no — no – no " - ")`,
      `- No subject line in the output — body only`,
      `- No corporate words: leverage, utilize, synergy, streamline, robust, endeavor, facilitate`,
      `- Write like a real person, short sentences, plain English`,
      `- Do NOT write generic fluff — be specific about what you sell`,
    ].join('\n');
  }

  async _generateSubject(lead) {
    const variants = this.config.sequence_templates?.step_1?.subject_variants ?? [];
    try {
      const raw = await chat({
        system: this._buildSystemPrompt(),
        messages: [{ role: 'user', content:
          `Write one cold email subject line for this prospect.\n\n` +
          `Prospect: ${lead.name ?? 'the owner'} at ${lead.company ?? 'their business'} (${lead.industry ?? ''}, ${lead.location ?? ''})\n` +
          (lead.site_tagline ? `Their site: "${lead.site_tagline}"\n` : '') +
          (variants.length ? `\nStyle examples (do not copy verbatim): ${variants.join(' | ')}\n` : '') +
          `\nRules for the subject line:\n` +
          `- One line, under 50 characters\n` +
          `- Do NOT include any person's name — not the prospect's, not Derek's\n` +
          `- Do NOT use questions\n` +
          `- Focus on the business or the problem, not the person\n` +
          `- Examples of good style: "after-hours leads for dental offices" | "quick thought on your website" | "AI chatbot for ${lead.industry ?? 'your business'}"\n` +
          `- No punctuation at the end`,
        }],
        max_tokens: 50,
      });
      // Take only the first line — LLM sometimes bleeds signature into output
      const firstLine = raw.split('\n')[0].replace(/^"|"$/g, '').trim();
      return this._sanitizeCopy(firstLine);
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

    const stepKey = `step_${step + 1}`;
    const tmpl    = this.config.sequence_templates?.[stepKey] ?? {};
    const stepInstructions = [
      tmpl.hook ? `Hook guidance: ${tmpl.hook}` : null,
      tmpl.body ? `Body guidance: ${tmpl.body}` : null,
      tmpl.cta  ? `CTA guidance: ${tmpl.cta}`   : null,
    ].filter(Boolean).join('\n');

    try {
      const body = await chat({
        system: this._buildSystemPrompt(),
        messages: [{ role: 'user', content:
          `Write email step ${step + 1} of ${this.config.sequence_length ?? 3} for this prospect.\n\n` +
          `Name: ${lead.name ?? 'there'}\n` +
          `Company: ${lead.company ?? '(unknown)'}\n` +
          `Industry: ${lead.industry ?? ''}\n` +
          `Location: ${lead.location ?? ''}\n` +
          (lead.site_tagline ? `Their website says: "${lead.site_tagline}"\n` : '') +
          (stepInstructions ? `\nStep guidance:\n${stepInstructions}\n` : '') +
          `\nIf their site tagline is provided, open with one specific sentence referencing their actual business. Then pitch the chatbot. Then mention the free trial and the website. End with the sign-off.`,
        }],
        max_tokens: 300,
      });
      return this._sanitizeCopy(body);
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
