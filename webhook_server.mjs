/**
 * webhook_server.mjs
 *
 * Standalone HTTP server that handles inbound events for the email system.
 *
 * Routes:
 *   POST /webhook/brevo   — Brevo send/tracking events (open, click, bounce, spam,
 *                           unsubscribe, inbound_email). Verifies HMAC sig if
 *                           BREVO_WEBHOOK_SECRET is set.
 *   GET  /unsubscribe     — One-click unsubscribe from email footer (signed URL)
 *   POST /unsubscribe     — RFC 8058 machine-triggered one-click unsubscribe
 *   GET  /health          — Health check (for load balancers / uptime monitors)
 *
 * IMAP reply poller runs in the same process, checking for new replies every
 * IMAP_POLL_INTERVAL ms and routing them through processInboundReply().
 *
 * Usage:
 *   node webhook_server.mjs
 *
 * Required env vars:
 *   UNSUB_SECRET       — HMAC secret (must match EMAIL_NODE's UNSUB_SECRET)
 *   UNSUB_BASE_URL     — public base URL this server is reachable at
 *
 * Optional env vars:
 *   WEBHOOK_PORT           — HTTP port (default 3001)
 *   BREVO_WEBHOOK_SECRET   — verifies X-Brevo-Signature on every POST
 *   IMAP_HOST, IMAP_PORT, IMAP_USER, IMAP_PASS, IMAP_SECURE
 *   IMAP_POLL_INTERVAL     — ms between IMAP polls (default 60000)
 */

import { config }    from 'dotenv';
config();

import express       from 'express';
import crypto        from 'crypto';
import { ImapFlow }  from 'imapflow';
import { EmailNode } from './nodes/email_node.mjs';
import { log }       from './core/logger.mjs';

const PORT            = Number(process.env.WEBHOOK_PORT         ?? 3001);
const BREVO_SECRET    = process.env.BREVO_WEBHOOK_SECRET        ?? '';
const BREVO_API_KEY   = process.env.BREVO_API_KEY               ?? '';
const IMAP_POLL_MS    = Number(process.env.IMAP_POLL_INTERVAL   ?? 60_000);
const BREVO_POLL_MS   = Number(process.env.BREVO_POLL_INTERVAL  ?? 5 * 60_000);
const IMAP_CONFIGURED = !!(process.env.IMAP_HOST && process.env.IMAP_USER && process.env.IMAP_PASS);

// Minimal EmailNode — not started as a cycle-running node.
// Used only for: memory access, handleBrevoEvent(), verifyUnsubToken(), processInboundReply().
const emailNode = new EmailNode(
  {
    node_id:             'webhook-server',
    from_email:          process.env.SES_FROM_EMAIL  ?? 'derek@heinrichstech.com',
    from_name:           process.env.SES_FROM_NAME   ?? 'Derek',
    unsub_base_url:      process.env.UNSUB_BASE_URL  ?? 'https://heinrichstech.com/unsubscribe',
    allow_self_destruct: false,
  },
  process.env.AWS_REGION ?? 'us-east-1',
);

// ── Express setup ─────────────────────────────────────────────────────────────

const app = express();

// Capture raw body for Brevo HMAC verification before parsing JSON.
// Also handles cases where Content-Type is missing or wrong.
app.use((req, _res, next) => {
  const chunks = [];
  req.on('data', chunk => chunks.push(chunk));
  req.on('end', () => {
    const raw = Buffer.concat(chunks);
    req.rawBody = raw.toString('utf8');
    if (raw.length) {
      try { req.body = JSON.parse(req.rawBody); } catch { req.body = {}; }
    } else {
      req.body = {};
    }
    next();
  });
});

// ── Brevo signature verification ──────────────────────────────────────────────

function verifyBrevoSig(rawBody, sigHeader) {
  if (!BREVO_SECRET)  return true;   // Skip if not configured
  if (!sigHeader)     return false;
  const expected = crypto
    .createHmac('sha256', BREVO_SECRET)
    .update(rawBody, 'utf8')
    .digest('hex');
  try {
    return crypto.timingSafeEqual(
      Buffer.from(sigHeader, 'hex'),
      Buffer.from(expected, 'hex'),
    );
  } catch {
    return false;
  }
}

// ── POST /webhook/brevo ───────────────────────────────────────────────────────

app.post('/webhook/brevo', async (req, res) => {
  const sig = req.headers['x-brevo-signature'] ?? req.headers['x-sendinblue-signature'] ?? '';

  if (BREVO_SECRET && !verifyBrevoSig(req.rawBody, sig)) {
    log.warn({ event: 'brevo_sig_reject' });
    return res.status(401).json({ error: 'Invalid signature' });
  }

  // Respond immediately — Brevo retries if no 200 within a few seconds
  res.status(200).json({ ok: true });

  // Brevo may send a single event object or an array of events
  const events = Array.isArray(req.body) ? req.body : [req.body];

  for (const event of events) {
    try {
      log.info({ event: 'brevo_event_received', type: event.event, email: event.email });
      await emailNode.handleBrevoEvent(event);
    } catch (err) {
      log.error({ event: 'brevo_event_error', error: err.message, type: event?.event });
    }
  }
});

// ── GET /unsubscribe — human-initiated from email footer link ─────────────────

app.get('/unsubscribe', async (req, res) => {
  const { id, t } = req.query;

  if (!id || !t) {
    return res.status(400).send(unsubPage('Invalid unsubscribe link.'));
  }

  if (!emailNode.verifyUnsubToken(String(id), String(t))) {
    log.warn({ event: 'unsub_bad_token', lead_id: id });
    return res.status(403).send(unsubPage('This link is invalid or has expired.'));
  }

  try {
    await emailNode.memory.suppressLead(String(id), 'unsubscribed');
    emailNode.increment('unsubscribes');
    log.info({ event: 'unsub_confirmed', lead_id: id });
    res.status(200).send(unsubPage("You've been unsubscribed. You won't hear from us again.", true));
  } catch (err) {
    log.error({ event: 'unsub_error', error: err.message });
    res.status(500).send(unsubPage('Something went wrong. Please reply STOP to unsubscribe.'));
  }
});

// ── POST /unsubscribe — RFC 8058 one-click (mail clients send this automatically) ──

app.post('/unsubscribe', async (req, res) => {
  const id = req.body?.id ?? req.query?.id;
  const t  = req.body?.t  ?? req.query?.t;

  if (!id || !t || !emailNode.verifyUnsubToken(String(id), String(t))) {
    return res.status(403).json({ error: 'Invalid token' });
  }

  try {
    await emailNode.memory.suppressLead(String(id), 'unsubscribed');
    emailNode.increment('unsubscribes');
    log.info({ event: 'unsub_oneclick', lead_id: id });
    res.status(200).json({ ok: true });
  } catch (err) {
    log.error({ event: 'unsub_oneclick_error', error: err.message });
    res.status(500).json({ error: 'Failed' });
  }
});

// ── GET /health ───────────────────────────────────────────────────────────────

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', ts: Date.now(), imap: IMAP_CONFIGURED });
});

// ── Unsubscribe confirmation HTML ─────────────────────────────────────────────

function unsubPage(message, success = false) {
  const icon  = success ? '&#10003;' : '&#9888;';
  const color = success ? '#22c55e'  : '#ef4444';
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Unsubscribe</title>
  <style>
    body { margin: 0; font-family: Arial, sans-serif; background: #f4f4f5;
           display: flex; align-items: center; justify-content: center; min-height: 100vh; }
    .card { background: #fff; border-radius: 12px; padding: 48px 40px;
            box-shadow: 0 2px 16px rgba(0,0,0,.08); text-align: center; max-width: 420px; width: 90%; }
    .icon { font-size: 56px; color: ${color}; line-height: 1; }
    h2 { margin: 16px 0 8px; color: #111; font-size: 20px; font-weight: 600; }
    p { color: #888; font-size: 13px; margin: 0; }
  </style>
</head>
<body>
  <div class="card">
    <div class="icon">${icon}</div>
    <h2>${message}</h2>
    <p>Heinrichs Software Solutions</p>
  </div>
</body>
</html>`;
}

// ── Brevo events poller ───────────────────────────────────────────────────────
// Transactional webhooks don't support click/open events on this plan, so we
// poll the REST API every BREVO_POLL_MS and route events through handleBrevoEvent.

const BREVO_EVENT_MAP = {
  clicks:          'click',
  opens:           'opened',
  hardBounces:     'hard_bounce',
  softBounces:     'soft_bounce',
  spam:            'spam',
  unsubscriptions: 'unsubscribe',
};

const _seenBrevoEvents = new Set();

async function pollBrevoEvents() {
  if (!BREVO_API_KEY) return;
  const today    = new Date().toISOString().slice(0, 10);
  const tomorrow = new Date(Date.now() + 86400000).toISOString().slice(0, 10);

  for (const [apiEvent, handlerEvent] of Object.entries(BREVO_EVENT_MAP)) {
    try {
      const url  = `https://api.brevo.com/v3/smtp/statistics/events?event=${apiEvent}&startDate=${today}&endDate=${tomorrow}&limit=100`;
      const resp = await fetch(url, { headers: { 'api-key': BREVO_API_KEY } });
      if (!resp.ok) continue;
      const { events = [] } = await resp.json();

      for (const ev of events) {
        const key = `${ev.messageId}::${handlerEvent}::${ev.email}`;
        if (_seenBrevoEvents.has(key)) continue;
        _seenBrevoEvents.add(key);

        // Don't count unsubscribe-link clicks as real CTA clicks
        if (handlerEvent === 'click' && ev.link?.includes('/unsubscribe')) continue;

        await emailNode.handleBrevoEvent({ event: handlerEvent, email: ev.email });
        log.info({ event: 'brevo_poll_synced', type: handlerEvent, email: ev.email });
      }
    } catch (err) {
      log.error({ event: 'brevo_poll_error', apiEvent, error: err.message });
    }
  }
}

// ── IMAP reply poller ─────────────────────────────────────────────────────────

async function pollImap() {
  const client = new ImapFlow({
    host:   process.env.IMAP_HOST,
    port:   Number(process.env.IMAP_PORT ?? 993),
    secure: (process.env.IMAP_SECURE ?? 'true') !== 'false',
    auth: {
      user: process.env.IMAP_USER,
      pass: process.env.IMAP_PASS,
    },
    logger: false,
  });

  try {
    await client.connect();
    const lock = await client.getMailboxLock('INBOX');
    let processed = 0;

    try {
      // Fetch all unseen messages
      for await (const msg of client.fetch({ unseen: true }, { envelope: true, source: true })) {
        const from   = msg.envelope?.from?.[0];
        const sender = from ? `${from.mailbox}@${from.host}` : null;
        if (!sender) continue;

        const subject  = msg.envelope.subject ?? '';
        const bodyText = msg.source ? msg.source.toString('utf8') : '';

        await emailNode.processInboundReply(sender, subject, bodyText, null);

        // Mark processed message as seen so we don't re-process it
        await client.messageFlagsAdd({ uid: msg.uid }, ['\\Seen'], { uid: true });
        processed++;
      }
    } finally {
      lock.release();
    }

    if (processed > 0) {
      log.info({ event: 'imap_poll_complete', processed });
    }

    await client.logout();
  } catch (err) {
    log.error({ event: 'imap_poll_error', error: err.message });
    try { await client.close(); } catch {}
  }
}

// ── Start ─────────────────────────────────────────────────────────────────────

app.listen(PORT, () => {
  const imapLine = IMAP_CONFIGURED
    ? `${process.env.IMAP_HOST} poll:${IMAP_POLL_MS / 1000}s`
    : 'disabled — set IMAP_HOST/USER/PASS';

  console.log(`
╔══════════════════════════════════════════════════════╗
║          SWARM WEBHOOK SERVER                        ║
╠══════════════════════════════════════════════════════╣
║  POST /webhook/brevo   Brevo event handler           ║
║  GET  /unsubscribe     One-click unsubscribe         ║
║  POST /unsubscribe     RFC 8058 machine unsub        ║
║  GET  /health          Health check                  ║
╠══════════════════════════════════════════════════════╣
║  Port : ${String(PORT).padEnd(44)}║
║  IMAP : ${imapLine.slice(0, 44).padEnd(44)}║
╚══════════════════════════════════════════════════════╝
`);
  log.info({ event: 'webhook_server_started', port: PORT });
});

if (IMAP_CONFIGURED) {
  pollImap();
  setInterval(pollImap, IMAP_POLL_MS);
} else {
  log.info({ event: 'imap_disabled', reason: 'IMAP_HOST/USER/PASS not configured' });
}

if (BREVO_API_KEY) {
  pollBrevoEvents();
  setInterval(pollBrevoEvents, BREVO_POLL_MS);
  log.info({ event: 'brevo_poller_started', interval_ms: BREVO_POLL_MS });
} else {
  log.warn({ event: 'brevo_poller_disabled', reason: 'BREVO_API_KEY not set' });
}
