/**
 * EmailNode — Professional cold email sequencing engine.
 *
 * Features:
 *   - Multi-step drip sequences with full lead context (about_text, hours, pain points)
 *   - Lead score prioritization — hot leads (≥60) always go first
 *   - Email threading — follow-ups arrive in the same Gmail/Outlook thread
 *   - Human-like send pacing — 45–180s random gaps between sends
 *   - Warm-up schedule — ramps from 20→max over 4 weeks to protect Brevo rep
 *   - Signed URL unsubscribe — one-click, no mailto hacks
 *   - Bounce/complaint auto-suppression via SES→SNS→SQS or Brevo webhook
 *   - CAN-SPAM + GDPR compliant
 *   - Reply detection (human vs auto-reply)
 *   - Rate monitoring — auto-pauses at 2% bounce or 0.1% complaint
 */

import nodemailer                                     from 'nodemailer';
import crypto                                        from 'crypto';
import { SQSClient, ReceiveMessageCommand,
         DeleteMessageCommand }                       from '@aws-sdk/client-sqs';
import { chat }                                       from '../core/llm.mjs';
import { BaseNode }                                   from '../core/base_node.mjs';
import { log }                                        from '../core/logger.mjs';

const sleep = ms => new Promise(r => setTimeout(r, ms));
const jitter = (base, spread) => Math.floor(base + Math.random() * spread);

const AUTO_REPLY_PATTERNS = [
  /out of office/i, /auto.?reply/i, /automatic response/i,
  /vacation/i, /away from (the )?office/i, /do not reply/i,
  /no.?reply@/i, /noreply@/i, /autoresponder/i, /on annual leave/i,
  /currently unavailable/i, /will be back/i,
];

const OPT_OUT_PATTERNS = [
  /\bstop\b/i, /\bunsubscribe\b/i, /\bremove me\b/i,
  /\bopt.?out\b/i, /\btake me off\b/i, /\bno more emails?\b/i,
  /\bdo not (contact|email)\b/i, /\bnot interested\b/i,
  /\bplease remove\b/i, /\bdon't (contact|email)\b/i,
];

// ── Warm-up schedule: max emails per day by week ──────────────────────────────
const WARMUP_SCHEDULE = [
  20,   // week 1
  40,   // week 2
  80,   // week 3
  120,  // week 4
  150,  // week 5+
];

export class EmailNode extends BaseNode {
  static nodeType = 'email_node';

  constructor(config, region = 'us-east-1', parentId = null) {
    super(config, region, parentId);

    this.transporter = nodemailer.createTransport({
      host:   process.env.BREVO_SMTP_HOST ?? 'smtp-relay.brevo.com',
      port:   Number(process.env.BREVO_SMTP_PORT ?? 587),
      secure: false,
      auth: {
        user: process.env.BREVO_SMTP_USER,
        pass: process.env.BREVO_SMTP_PASS,
      },
    });

    this.sqsFeedback      = new SQSClient({ region });
    this.fromEmail        = config.from_email  ?? process.env.SES_FROM_EMAIL ?? 'derek@heinrichstech.com';
    this.fromName         = config.from_name   ?? process.env.SES_FROM_NAME  ?? 'Derek';
    this.replyToEmail     = config.reply_to    ?? this.fromEmail;
    this.sendingDomain    = this.fromEmail.split('@')[1] ?? 'heinrichstech.com';

    this.sequenceSteps    = config.sequence_steps    ?? 3;
    this.followUpDays     = config.follow_up_intervals ?? [3, 5, 7];
    this.maxDailyLimit    = config.daily_send_limit  ?? 150;
    this.subjectLines     = config.subject_lines     ?? ['Quick question', 'Noticed something'];

    // Warm-up: track when this node first started sending
    this.warmupStartDate  = config.warmup_start_date
      ?? process.env.EMAIL_WARMUP_START
      ?? new Date().toISOString().slice(0, 10);

    this._sentToday = 0;
    this._dayStart  = new Date().toDateString();

    // Compliance — bounce/complaint feedback queue (SES→SNS→SQS)
    this.feedbackQueueUrl = config.feedback_queue_url
      ?? process.env.SWARM_SES_FEEDBACK_QUEUE_URL ?? '';

    // Send window
    this.sendWindowStart = config.send_window_start ?? '08:00';
    this.sendWindowEnd   = config.send_window_end   ?? '17:00';
    this.sendDays        = config.send_days ?? ['Monday','Tuesday','Wednesday','Thursday','Friday'];

    // Unsubscribe URL base (webhook_server handles the GET request)
    this.unsubBaseUrl    = config.unsub_base_url
      ?? process.env.UNSUB_BASE_URL ?? 'https://heinrichstech.com/unsubscribe';

    // Token secret for signed unsubscribe links
    this._unsubSecret    = process.env.UNSUB_SECRET ?? 'swarm-unsub-secret-change-me';

    // Rate tracking
    this._bounces    = 0;
    this._complaints = 0;
    this._paused     = false;
  }

  // ── Daily warm-up cap ────────────────────────────────────────────────────────

  _warmupDailyCap() {
    const start   = new Date(this.warmupStartDate);
    const daysSince = Math.floor((Date.now() - start.getTime()) / 86400000);
    const weekIndex = Math.min(Math.floor(daysSince / 7), WARMUP_SCHEDULE.length - 1);
    const cap = WARMUP_SCHEDULE[Math.max(0, weekIndex)];
    return Math.min(cap, this.maxDailyLimit);
  }

  // ── Main cycle ───────────────────────────────────────────────────────────────

  async runCycle() {
    this._resetDailyCounterIfNeeded();
    await this._processFeedback();
    this._checkRateHealth();

    const tasks = await this.receiveTasks(10);
    for (const task of tasks) {
      await this._handleTask(task);
      await this.ackTask(task._receipt_handle);
    }

    await this._seedNewLeads();

    // Kill switch — set EMAIL_ENABLED=true in .env to allow sending
    if (process.env.EMAIL_ENABLED !== 'true') {
      log.info({ event: 'email_sending_disabled', reason: 'EMAIL_ENABLED is not set to true' });
      return;
    }

    if (this._paused) {
      log.warn({ event: 'email_auto_paused', bounces: this._bounces, complaints: this._complaints });
      return;
    }
    if (!this._isWithinSendWindow()) {
      log.info({ event: 'outside_send_window' });
      return;
    }

    const cap = this._warmupDailyCap();
    if (this._sentToday < cap) {
      log.info({ event: 'send_cycle_start', sentToday: this._sentToday, cap });
      await this._processPendingSequences(cap);
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
        if (task.key) this.config[task.key] = task.value;
        break;
    }
  }

  // ── Sequence management ──────────────────────────────────────────────────────

  async _launchCampaignSequence(campaign) {
    const leads = await this.memory.getLeads(campaign.target_audience, 1000);
    const now   = Math.floor(Date.now() / 1000);
    for (const lead of leads) {
      if (!lead.email || lead.sequence_state) continue;
      await this.memory.upsertLead({
        ...lead,
        sequence_state: { lead_id: lead.lead_id, campaign_id: campaign.campaign_id, step: 0, next_send_at: now, status: 'pending' },
      });
    }
  }

  async _seedNewLeads() {
    try {
      const all      = await this.memory.getLeads(null, 5000);
      const unseeded = all.filter(l =>
        l.email && !l.sequence_state && !l.unsubscribed && !l.bounced && !l.complained && !l.suppressed,
      );
      if (!unseeded.length) return;
      // Offset by 5 minutes so newly seeded leads never fire on the same cycle they're created.
      const sendAt = Math.floor(Date.now() / 1000) + 300;
      for (const lead of unseeded) {
        await this.memory.upsertLead({
          ...lead,
          sequence_state: { lead_id: lead.lead_id, step: 0, next_send_at: sendAt, status: 'pending' },
        });
      }
      log.info({ event: 'leads_seeded', count: unseeded.length });
    } catch (err) {
      log.error({ event: 'seed_leads_error', error: err.message });
    }
  }

  async _processPendingSequences(cap) {
    const now   = Math.floor(Date.now() / 1000);
    const leads = await this.memory.getLeads(null, 2000);

    const due = leads
      .filter(l =>
        l.sequence_state?.status === 'pending' &&
        (l.sequence_state?.next_send_at ?? Infinity) <= now &&
        !l.unsubscribed && !l.bounced && !l.complained && !l.replied_human,
      )
      .sort((a, b) => (b.lead_score ?? 0) - (a.lead_score ?? 0));

    // Follow-ups (step > 0) are replies in existing threads — send all due ones first,
    // they don't count against the daily cap.
    const followUps = due.filter(l => (l.sequence_state?.step ?? 0) > 0);
    const coldLeads = due.filter(l => (l.sequence_state?.step ?? 0) === 0);

    for (const lead of followUps) {
      await this._sendSequenceEmail(lead);
      const delay = jitter(45_000, 135_000);
      log.info({ event: 'send_pacing', next_in_ms: delay });
      await sleep(delay);
    }

    for (const lead of coldLeads) {
      if (this._sentToday >= cap) break;
      await this._sendSequenceEmail(lead);
      const delay = jitter(45_000, 135_000);
      log.info({ event: 'send_pacing', next_in_ms: delay });
      await sleep(delay);
    }
  }

  async _sendSequenceEmail(lead) {
    const seq  = lead.sequence_state ?? {};
    const step = seq.step ?? 0;

    if (step >= this.sequenceSteps) {
      await this.memory.upsertLead({ ...lead, sequence_state: { ...seq, status: 'completed' } });
      return;
    }

    const subject   = step === 0
      ? await this._generateSubject(lead)
      : `Re: ${seq.original_subject ?? await this._generateSubject(lead)}`;

    const body      = await this._generateEmailBody(lead, step);
    const html      = this._buildHtmlEmail(body, lead);
    const plainText = body.replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim();

    // RFC-compliant Message-ID for threading
    const messageId = `<${crypto.randomUUID()}@${this.sendingDomain}>`;

    // Build thread headers for follow-ups
    const threadHeaders = {};
    if (step > 0 && seq.message_ids?.length) {
      const lastId = seq.message_ids[seq.message_ids.length - 1];
      threadHeaders['In-Reply-To'] = lastId;
      threadHeaders['References']  = seq.message_ids.join(' ');
    }

    try {
      await this.transporter.sendMail({
        from:    `"${this.fromName}" <${this.fromEmail}>`,
        to:      lead.email,
        replyTo: this.replyToEmail,
        subject,
        html,
        text:    plainText,
        headers: {
          'Message-ID':            messageId,
          'List-Unsubscribe':      `<${this._signedUnsubUrl(lead.lead_id)}>, <mailto:${this.replyToEmail}?subject=unsubscribe>`,
          'List-Unsubscribe-Post': 'List-Unsubscribe=One-Click',
          ...threadHeaders,
        },
      });

      const nextInterval   = this.followUpDays[Math.min(step, this.followUpDays.length - 1)];
      const updatedMsgIds  = [...(seq.message_ids ?? []), messageId];

      await this.memory.upsertLead({
        ...lead,
        sequence_state: {
          ...seq,
          step:             step + 1,
          next_send_at:     Math.floor(Date.now() / 1000) + nextInterval * 86400,
          status:           (step + 1) < this.sequenceSteps ? 'pending' : 'completed',
          original_subject: seq.original_subject ?? subject,
          message_ids:      updatedMsgIds,
          last_sent_at:     new Date().toISOString(),
        },
      });

      this.increment('emails_sent');
      if (step === 0) this._sentToday++;  // only cold outreach counts against daily cap
      await this.memory.writeMetric(this.nodeId, this.nodeType, 'emails_sent', 1);

      console.log(`  [EMAIL] step ${step + 1}/${this.sequenceSteps} → ${lead.email} | "${subject}" | score:${lead.lead_score ?? 0}`);
      log.info({ event: 'email_sent', step: step + 1, to: lead.email, company: lead.company ?? '?', subject });

    } catch (err) {
      log.error({ event: 'email_send_error', lead_id: lead.lead_id, error: err.message });
      this.increment('errors');
    }
  }

  // ── Email construction ───────────────────────────────────────────────────────

  _buildHtmlEmail(bodyText, lead) {
    const unsubUrl = this._signedUnsubUrl(lead.lead_id);
    // Plain-text style HTML — higher deliverability than fancy HTML
    return [
      `<div style="font-family:Arial,sans-serif;font-size:14px;color:#222;line-height:1.6;max-width:600px;">`,
      bodyText
        .split('\n')
        .map(line => line.trim() ? `<p style="margin:0 0 12px 0;">${line}</p>` : '')
        .join(''),
      `<br>`,
      `<div style="font-size:11px;color:#aaa;border-top:1px solid #eee;padding-top:8px;margin-top:20px;">`,
      `You're receiving this because we thought our service might help ${lead.company ?? 'your business'}.`,
      `If you'd like to opt out, <a href="${unsubUrl}" style="color:#aaa;">click here to unsubscribe</a>`,
      `or reply <strong>STOP</strong>.`,
      `<br>Heinrichs Software Solutions &bull; Florida, USA`,
      `</div>`,
      `</div>`,
    ].join('\n');
  }

  _signedUnsubUrl(leadId) {
    const token = crypto
      .createHmac('sha256', this._unsubSecret)
      .update(leadId)
      .digest('hex')
      .slice(0, 16);
    return `${this.unsubBaseUrl}?id=${leadId}&t=${token}`;
  }

  verifyUnsubToken(leadId, token) {
    const expected = crypto
      .createHmac('sha256', this._unsubSecret)
      .update(leadId)
      .digest('hex')
      .slice(0, 16);
    return token === expected;
  }

  // ── LLM email generation ─────────────────────────────────────────────────────

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
      `SENDER:`,
      ...(c.differentiators?.map(d => `- ${d}`) ?? []),
      ``,
      `SIGN OFF EVERY EMAIL WITH EXACTLY THIS:`,
      c.signature ?? c.from_name ?? this.fromName,
      ``,
      `RULES — NEVER BREAK:`,
      `- Under 120 words total`,
      `- No dashes of any kind (no — no – no " - ")`,
      `- No subject line in output, body only`,
      `- No corporate buzzwords: leverage, utilize, synergy, streamline, robust, facilitate`,
      `- Write like a real human, short sentences, plain English`,
      `- If you have the prospect's first name, use it naturally ONCE in the email`,
      `- If you know what their business does, reference it specifically in the first sentence`,
      `- If they have an after-hours gap, that is your hook — they are losing leads right now`,
      `- If they have no contact form, that makes the urgency even higher`,
      `- Always mention the free trial and the website`,
    ].join('\n');
  }

  _buildLeadContext(lead) {
    const lines = [];

    if (lead.first_name)         lines.push(`First name: ${lead.first_name}`);
    if (lead.company)            lines.push(`Business name: ${lead.company}`);
    if (lead.industry)           lines.push(`Industry: ${lead.industry}`);
    if (lead.location)           lines.push(`Location: ${lead.location}`);
    if (lead.website)            lines.push(`Their website: ${lead.website}`);
    if (lead.site_tagline)       lines.push(`Their tagline: "${lead.site_tagline}"`);
    if (lead.about_text)         lines.push(`About them: "${lead.about_text}"`);
    if (lead.business_hours)     lines.push(`Business hours: ${lead.business_hours}`);
    if (lead.has_after_hours_gap === true)  lines.push(`After-hours gap: YES — they are closed evenings/weekends and losing leads`);
    if (lead.has_after_hours_gap === false) lines.push(`After-hours gap: No — they appear to have extended hours`);
    if (lead.has_contact_form === false)    lines.push(`Contact form: NONE — visitors have no way to reach them outside business hours`);
    if (lead.has_contact_form === true)     lines.push(`Contact form: Yes — they have a form but no live response`);
    if (lead.has_booking_widget)            lines.push(`Booking widget: Yes — they already use scheduling software (tech-forward)`);
    if (lead.site_platform && lead.site_platform !== 'custom') lines.push(`Site platform: ${lead.site_platform}`);
    if (lead.rating)             lines.push(`Google rating: ${lead.rating} stars (${lead.review_count ?? '?'} reviews)`);
    if (lead.lead_score)         lines.push(`Lead score: ${lead.lead_score}/100`);

    return lines.join('\n');
  }

  async _generateSubject(lead) {
    const variants = this.config.sequence_templates?.step_1?.subject_variants ?? [];
    try {
      const raw = await chat({
        system: this._buildSystemPrompt(),
        messages: [{ role: 'user', content:
          `Write ONE cold email subject line for this prospect.\n\n` +
          `PROSPECT INFO:\n${this._buildLeadContext(lead)}\n\n` +
          (variants.length ? `Style examples (do NOT copy verbatim): ${variants.join(' | ')}\n\n` : '') +
          `RULES:\n` +
          `- One line only, under 50 characters\n` +
          `- No question marks\n` +
          `- Reference their business or industry specifically\n` +
          `- No punctuation at the end\n` +
          `- Output the subject line only, nothing else`,
        }],
        max_tokens: 60,
      });
      return this._sanitizeCopy(raw.split('\n')[0].replace(/^"|"$/g, '').trim());
    } catch {
      return this.subjectLines[0];
    }
  }

  async _generateEmailBody(lead, step) {
    const stepKey = `step_${step + 1}`;
    const tmpl    = this.config.sequence_templates?.[stepKey] ?? {};

    const stepGuidance = [
      tmpl.hook ? `Hook: ${tmpl.hook}` : null,
      tmpl.body ? `Body: ${tmpl.body}` : null,
      tmpl.cta  ? `CTA: ${tmpl.cta}`  : null,
    ].filter(Boolean).join('\n');

    const stepContext = [
      `This is email ${step + 1} of ${this.sequenceSteps} in the sequence.`,
      step === 0 ? `First touch — introduce yourself, reference their business, pitch the chatbot, mention free trial.` : null,
      step === 1 ? `Follow-up — they didn't reply. New angle. Keep it short. Single yes/no question to reduce friction.` : null,
      step === 2 ? `Final email — honest breakup email. Respect their time. Leave door open. No hard pitch. Just leave the website.` : null,
    ].filter(Boolean).join(' ');

    try {
      const body = await chat({
        system: this._buildSystemPrompt(),
        messages: [{ role: 'user', content:
          `${stepContext}\n\n` +
          `PROSPECT INFO:\n${this._buildLeadContext(lead)}\n\n` +
          (stepGuidance ? `GUIDANCE FOR THIS STEP:\n${stepGuidance}\n\n` : '') +
          `Write the email body now. No subject line. Sign off with the signature. Under 120 words.`,
        }],
        max_tokens: 350,
      });
      return this._sanitizeCopy(body);
    } catch {
      return `Hey${lead.first_name ? ' ' + lead.first_name : ''}, wanted to reach out about your website. We build AI chatbots for businesses like yours. 14-day free trial at heinrichstech.com.\n\n${this.fromName}`;
    }
  }

  _sanitizeCopy(text) {
    return text
      .replace(/\s*\u2014\s*/g, ' ')
      .replace(/\s*\u2013\s*/g, ' ')
      .replace(/\s+-\s+/g, ' ')
      .replace(/\s{2,}/g, ' ')
      .trim();
  }

  // ── Reply processing ─────────────────────────────────────────────────────────

  async processInboundReply(sender, subject, body, leadId) {
    const isAuto   = AUTO_REPLY_PATTERNS.some(p => p.test(body) || p.test(subject));
    const isOptOut = OPT_OUT_PATTERNS.some(p => p.test(body) || p.test(subject));

    const leads = leadId
      ? await this.memory.getLeads({ lead_id: leadId }, 1)
      : await this.memory.getLeads({ email: sender }, 1);

    if (!leads.length) return;
    const lead = leads[0];

    if (isOptOut) {
      await this.memory.suppressLead(lead.lead_id, 'unsubscribed');
      this.increment('unsubscribes');
      await this.memory.writeMetric(this.nodeId, this.nodeType, 'unsubscribes', 1);
      log.info({ event: 'opt_out', sender, lead_id: lead.lead_id });
      return;
    }

    if (isAuto) {
      this.increment('auto_replies');
      log.info({ event: 'auto_reply', sender });
      return;
    }

    // Human reply — pause the sequence, flag for follow-up
    this.increment('human_replies');
    await this.memory.writeMetric(this.nodeId, this.nodeType, 'human_replies', 1);
    await this.memory.upsertLead({
      ...lead,
      replied_human:  true,
      replied_at:     new Date().toISOString(),
      reply_snippet:  body.slice(0, 300),
      sequence_state: { ...lead.sequence_state, status: 'replied' },
    });
    console.log(`  [REPLY] Human reply from ${sender} (${lead.company ?? '?'}) — sequence paused`);
    log.info({ event: 'human_reply', sender, lead_id: lead.lead_id, company: lead.company });
  }

  // ── Bounce / complaint processing ────────────────────────────────────────────

  async _processFeedback() {
    if (!this.feedbackQueueUrl) return;
    try {
      const resp = await this.sqsFeedback.send(new ReceiveMessageCommand({
        QueueUrl: this.feedbackQueueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1,
      }));
      for (const msg of resp.Messages ?? []) {
        try {
          const sns = JSON.parse(msg.Body);
          const ses = JSON.parse(sns.Message);
          await this._handleSesEvent(ses);
        } catch (e) {
          log.warn({ event: 'feedback_parse_error', error: e.message });
        }
        await this.sqsFeedback.send(new DeleteMessageCommand({
          QueueUrl: this.feedbackQueueUrl, ReceiptHandle: msg.ReceiptHandle,
        }));
      }
    } catch (err) {
      log.error({ event: 'feedback_poll_error', error: err.message });
    }
  }

  async _handleSesEvent(sesEvent) {
    const type = sesEvent.notificationType;

    if (type === 'Bounce') {
      const bounce = sesEvent.bounce ?? {};
      for (const r of bounce.bouncedRecipients ?? []) {
        this._bounces++;
        if (bounce.bounceType === 'Permanent') {
          await this.memory.suppressByEmail(r.emailAddress, 'bounced');
          this.increment('hard_bounces');
        } else {
          const leads = await this.memory.getLeads({ email: r.emailAddress }, 1);
          if (leads[0]) {
            const count = (leads[0].soft_bounce_count ?? 0) + 1;
            if (count >= 3) await this.memory.suppressLead(leads[0].lead_id, 'bounced');
            else await this.memory.upsertLead({ ...leads[0], soft_bounce_count: count });
          }
          this.increment('soft_bounces');
        }
      }
    }

    if (type === 'Complaint') {
      for (const r of sesEvent.complaint?.complainedRecipients ?? []) {
        this._complaints++;
        await this.memory.suppressByEmail(r.emailAddress, 'complained');
        this.increment('complaints');
      }
    }
  }

  // ── Brevo webhook event handler ──────────────────────────────────────────────

  async handleBrevoEvent(event) {
    const email = event.email;
    if (!email) return;

    const leads = await this.memory.getLeads({ email }, 1);
    const lead  = leads[0];

    switch (event.event) {
      case 'hard_bounce':
      case 'invalid_email':
        this._bounces++;
        this.increment('hard_bounces');
        await this.memory.suppressByEmail(email, 'bounced');
        log.warn({ event: 'brevo_hard_bounce', email });
        break;

      case 'soft_bounce':
      case 'deferred':
        this._bounces++;
        this.increment('soft_bounces');
        if (lead) {
          const count = (lead.soft_bounce_count ?? 0) + 1;
          if (count >= 3) await this.memory.suppressLead(lead.lead_id, 'bounced');
          else await this.memory.upsertLead({ ...lead, soft_bounce_count: count });
        }
        break;

      case 'spam':
      case 'complaint':
        this._complaints++;
        this.increment('complaints');
        await this.memory.suppressByEmail(email, 'complained');
        log.warn({ event: 'brevo_spam_complaint', email });
        break;

      case 'unsubscribe':
        if (lead) await this.memory.suppressLead(lead.lead_id, 'unsubscribed');
        this.increment('unsubscribes');
        log.info({ event: 'brevo_unsubscribe', email });
        break;

      case 'opened':
        if (lead) await this.memory.upsertLead({ ...lead, last_opened_at: new Date().toISOString() });
        this.increment('opens');
        break;

      case 'click':
        if (lead) await this.memory.upsertLead({ ...lead, last_clicked_at: new Date().toISOString() });
        this.increment('clicks');
        break;

      case 'inbound_email':
        if (lead) await this.processInboundReply(email, event.subject ?? '', event.rawHtmlBody ?? event.text ?? '', lead.lead_id);
        break;
    }

    this._checkRateHealth();
  }

  // ── Helpers ──────────────────────────────────────────────────────────────────

  _isWithinSendWindow() {
    const now  = new Date();
    const opts = { timeZone: 'America/New_York', hour12: false };
    const day  = now.toLocaleDateString('en-US', { ...opts, weekday: 'long' });
    const hhmm = now.toLocaleTimeString('en-US', { ...opts, hour: '2-digit', minute: '2-digit' });
    if (!this.sendDays.includes(day)) return false;
    const cur   = hhmm.replace(':', '');
    const start = this.sendWindowStart.replace(':', '');
    const end   = this.sendWindowEnd.replace(':', '');
    return cur >= start && cur <= end;
  }

  _checkRateHealth() {
    const sent = this.getCounter('emails_sent');
    if (sent < 50) return;
    if (this._bounces    / sent > 0.02)  { this._paused = true; log.error({ event: 'auto_pause_bounce',    rate: (this._bounces / sent).toFixed(4) }); }
    if (this._complaints / sent > 0.001) { this._paused = true; log.error({ event: 'auto_pause_complaint', rate: (this._complaints / sent).toFixed(4) }); }
    if (this._paused) this.memory.writeMetric(this.nodeId, this.nodeType, 'auto_paused', 1);
  }

  _resetDailyCounterIfNeeded() {
    const today = new Date().toDateString();
    if (today !== this._dayStart) {
      this._sentToday  = 0;
      this._bounces    = 0;
      this._complaints = 0;
      this._paused     = false;
      this._dayStart   = today;
    }
  }

  // ── Metrics ──────────────────────────────────────────────────────────────────

  collectMetrics() {
    const sent   = this.getCounter('emails_sent');
    const humanR = this.getCounter('human_replies');
    return {
      emails_sent:    sent,
      human_replies:  humanR,
      opens:          this.getCounter('opens'),
      clicks:         this.getCounter('clicks'),
      reply_rate:     sent > 0 ? +(humanR / sent).toFixed(4) : 0,
      hard_bounces:   this.getCounter('hard_bounces'),
      soft_bounces:   this.getCounter('soft_bounces'),
      complaints:     this.getCounter('complaints'),
      unsubscribes:   this.getCounter('unsubscribes'),
      bounce_rate:    sent > 0 ? +(this._bounces    / sent).toFixed(4) : 0,
      complaint_rate: sent > 0 ? +(this._complaints / sent).toFixed(4) : 0,
      daily_cap:      this._warmupDailyCap(),
      auto_paused:    this._paused,
      warmup_week:    Math.floor((Date.now() - new Date(this.warmupStartDate).getTime()) / (7 * 86400000)) + 1,
    };
  }

  getImprovementContext() {
    return { config: this.config, sequence_steps: this.sequenceSteps, daily_cap: this._warmupDailyCap(), follow_up_days: this.followUpDays };
  }

  _computeScore(metrics) {
    return Math.min((metrics.reply_rate ?? 0) * 5, 1.0);
  }
}
