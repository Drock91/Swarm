/**
 * core/llm.mjs — Multi-provider LLM wrapper for the entire Swarm.
 *
 * Provider priority (fastest/cheapest first, paid as fallback):
 *   1. Groq       — Llama 3.3 70B, free tier, extremely fast
 *   2. Gemini     — 2.0 Flash, free tier, 1500 req/day
 *   3. Anthropic  — Claude, paid but high quality
 *   4. Grok       — xAI, paid fallback
 *   5. Mistral    — paid fallback
 *   6. OpenAI     — last resort
 *
 * All providers use the OpenAI-compatible SDK except Anthropic.
 * Automatic failover: if a provider errors or rate-limits, tries the next.
 */

import OpenAI    from 'openai';
import Anthropic from '@anthropic-ai/sdk';
import { log }   from './logger.mjs';

// ── OpenAI-compatible providers ──────────────────────────────────────────────
const PROVIDERS = [
  {
    name:    'groq',
    client:  () => new OpenAI({ baseURL: 'https://api.groq.com/openai/v1',                              apiKey: process.env.GROQ_API_KEY }),
    model:   'llama-3.3-70b-versatile',
    enabled: () => !!process.env.GROQ_API_KEY,
  },
  {
    name:    'gemini',
    client:  () => new OpenAI({ baseURL: 'https://generativelanguage.googleapis.com/v1beta/openai/',    apiKey: process.env.GOOGLE_API_KEY }),
    model:   'gemini-2.0-flash',
    enabled: () => !!process.env.GOOGLE_API_KEY,
  },
  {
    name:    'grok',
    client:  () => new OpenAI({ baseURL: 'https://api.x.ai/v1',                                        apiKey: process.env.GROK_API_KEY }),
    model:   'grok-2-latest',
    enabled: () => !!process.env.GROK_API_KEY,
  },
  {
    name:    'mistral',
    client:  () => new OpenAI({ baseURL: 'https://api.mistral.ai/v1',                                   apiKey: process.env.MISTRAL_API_KEY }),
    model:   'mistral-small-latest',
    enabled: () => !!process.env.MISTRAL_API_KEY,
  },
  {
    name:    'openai',
    client:  () => new OpenAI({ apiKey: process.env.OPENAI_API_KEY }),
    model:   process.env.OPENAI_MODEL ?? 'gpt-4o',
    enabled: () => !!process.env.OPENAI_API_KEY,
  },
];

// Anthropic uses its own SDK
const _anthropic = process.env.ANTHROPIC_API_KEY
  ? new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY })
  : null;

// Cache instantiated clients
const _clients = {};
function getClient(provider) {
  if (!_clients[provider.name]) _clients[provider.name] = provider.client();
  return _clients[provider.name];
}

// ── Core call with failover ───────────────────────────────────────────────────

async function _callWithFailover(system, messages, max_tokens) {
  const active = PROVIDERS.filter(p => p.enabled());

  // Try each OpenAI-compatible provider
  for (const provider of active) {
    try {
      const client = getClient(provider);
      const msgs = system
        ? [{ role: 'system', content: system }, ...messages]
        : messages;

      const resp = await client.chat.completions.create({
        model:      provider.model,
        messages:   msgs,
        max_tokens,
      });
      const text = resp.choices[0].message.content.trim();
      log.debug({ event: 'llm_call', provider: provider.name, model: provider.model });
      return text;
    } catch (err) {
      log.warn({ event: 'llm_provider_failed', provider: provider.name, error: err.message });
    }
  }

  // Final fallback: Anthropic
  if (_anthropic) {
    try {
      const params = { model: process.env.ANTHROPIC_MODEL ?? 'claude-opus-4-6', max_tokens, messages };
      if (system) params.system = system;
      const resp = await _anthropic.messages.create(params);
      log.debug({ event: 'llm_call', provider: 'anthropic' });
      return resp.content[0].text.trim();
    } catch (err) {
      log.error({ event: 'llm_anthropic_failed', error: err.message });
    }
  }

  throw new Error('All LLM providers failed');
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Plain text completion.
 * @param {{ system?: string, messages: {role:string,content:string}[], max_tokens?: number }} opts
 * @returns {Promise<string>}
 */
export async function chat({ system, messages, max_tokens = 1024 }) {
  return _callWithFailover(system, messages, max_tokens);
}

/**
 * JSON completion — strips code fences and parses the result.
 * @param {{ system?: string, messages: {role:string,content:string}[], max_tokens?: number }} opts
 * @returns {Promise<object>}
 */
export async function chatJSON({ system, messages, max_tokens = 1024 }) {
  const jsonSystem = [
    system ?? '',
    'Respond with valid JSON only. No markdown, no code fences, no explanation before or after the JSON.',
  ].filter(Boolean).join('\n\n');

  const text = await _callWithFailover(jsonSystem, messages, max_tokens);
  const cleaned = text.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/i, '').trim();
  return JSON.parse(cleaned);
}
