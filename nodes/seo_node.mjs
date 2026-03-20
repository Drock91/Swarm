/**
 * SEONode — Generates blog content, targets keywords, builds backlinks.
 *
 * Capabilities:
 *   - Long-form SEO article generation (OpenAI)
 *   - Keyword research via SerpAPI / GPT fallback
 *   - Publishes to WordPress REST API or S3
 *   - Self-improves keyword targeting based on ranking data
 *   - Scales to passive organic traffic machine
 */

import axios from 'axios';
import OpenAI from 'openai';
import { BaseNode } from '../core/base_node.mjs';
import { log } from '../core/logger.mjs';

const sleep = ms => new Promise(r => setTimeout(r, ms));

const SEO_ARTICLE_SYSTEM = `
You are an elite SEO content writer. Write compelling, deeply-researched articles that:
1. Target the primary keyword naturally throughout
2. Include semantic/LSI keywords
3. Follow E-E-A-T guidelines
4. Structure with H2/H3 headings, bullet points, and FAQ sections
5. Include a compelling meta description
Return JSON: { "title": "...", "meta_description": "...", "slug": "...", "content_html": "..." }
`.trim();

export class SEONode extends BaseNode {
  static nodeType = 'seo_node';

  constructor(config, region = 'us-east-1', parentId = null) {
    super(config, region, parentId);
    this.openai          = new OpenAI();
    this.serpApiKey      = config.serp_api_key ?? '';
    this.wpApiUrl        = config.wordpress_api_url ?? '';
    this.wpAuthToken     = config.wordpress_auth_token ?? '';
    this.targetNiche     = config.target_niche ?? 'digital marketing';
    this.articlesPerDay  = config.articles_per_day ?? 3;
    this.keywords        = config.seed_keywords ?? [];
    this._publishedToday = 0;
    this._dayStart       = new Date().toDateString();
  }

  async runCycle() {
    this._resetDailyCounter();
    const tasks = await this.receiveTasks(10);
    for (const t of tasks) {
      await this._handleTask(t);
      await this.ackTask(t._receipt_handle);
    }
    if (this._publishedToday < this.articlesPerDay) {
      const keywords = await this._discoverKeywords();
      const toWrite  = keywords.slice(0, Math.max(1, this.articlesPerDay - this._publishedToday));
      for (const kw of toWrite) {
        await this._writeAndPublish(kw);
      }
      await sleep(3_600_000); // pace to 1 batch/hour
    }
  }

  async _handleTask(task) {
    if (task.command === 'shutdown' && (!task.target_node_id || task.target_node_id === this.nodeId)) {
      await this.stop('commander_shutdown');
    } else if (task.command === 'run_campaign') {
      this.keywords.push(...(task.campaign?.keywords ?? []));
    } else if (task.command === 'update_config') {
      this.config[task.key] = task.value;
    }
  }

  async _discoverKeywords() {
    return this.serpApiKey
      ? this._serpKeywordResearch()
      : this._gptKeywordIdeas();
  }

  async _serpKeywordResearch() {
    try {
      const resp = await axios.get('https://serpapi.com/search', {
        params: {
          q:       `${this.targetNiche} ${this.keywords[0] ?? ''}`,
          api_key: this.serpApiKey,
          tbm:     'nws',
        },
        timeout: 15_000,
      });
      return (resp.data.news_results ?? []).slice(0, 5).map(r => r.title ?? '');
    } catch (err) {
      log.warn({ event: 'serp_error', error: err.message });
      return this._gptKeywordIdeas();
    }
  }

  async _gptKeywordIdeas() {
    const seeds = this.keywords.slice(0, 5).join(', ') || this.targetNiche;
    try {
      const resp = await this.openai.chat.completions.create({
        model:           'gpt-4o-mini',
        messages:        [{ role: 'user', content:
          `Give me 10 SEO article topic ideas for the niche: ${this.targetNiche}. ` +
          `Seed keywords: ${seeds}. Focus on long-tail, low-competition. ` +
          `Return JSON: { "topics": ["...", ...] }` }],
        response_format: { type: 'json_object' },
        temperature:     0.7,
      });
      const data = JSON.parse(resp.choices[0].message.content);
      const list = Array.isArray(data) ? data : (data.topics ?? Object.values(data)[0] ?? []);
      return list.slice(0, 10);
    } catch (err) {
      log.error({ event: 'keyword_gpt_error', error: err.message });
      return [`${this.targetNiche} tips`, `${this.targetNiche} guide`];
    }
  }

  async _writeAndPublish(keyword) {
    try {
      const article = await this._generateArticle(keyword);
      if (!article) return;

      const s3Key = `seo/${this.nodeId}/${article.slug}.json`;
      await this.memory.saveContent(s3Key, JSON.stringify(article));

      if (this.wpApiUrl && this.wpAuthToken) {
        await this._publishToWordPress(article);
      }

      this.increment('articles_published');
      this._publishedToday++;
      await this.memory.writeMetric(this.nodeId, this.nodeType, 'content_published', 1);
      log.info({ event: 'article_published', keyword, slug: article.slug });
    } catch (err) {
      log.error({ event: 'publish_error', keyword, error: err.message });
      this.increment('errors');
    }
  }

  async _generateArticle(keyword) {
    try {
      const resp = await this.openai.chat.completions.create({
        model:           'gpt-4o',
        messages:        [
          { role: 'system', content: SEO_ARTICLE_SYSTEM },
          { role: 'user',   content: `Write a 1500-word SEO article targeting: '${keyword}'\nNiche: ${this.targetNiche}` },
        ],
        response_format: { type: 'json_object' },
        temperature:     0.5,
      });
      return JSON.parse(resp.choices[0].message.content);
    } catch (err) {
      log.error({ event: 'article_gen_error', keyword, error: err.message });
      return null;
    }
  }

  async _publishToWordPress(article) {
    await axios.post(`${this.wpApiUrl}/wp-json/wp/v2/posts`, {
      title:   article.title,
      content: article.content_html,
      slug:    article.slug,
      status:  'publish',
      excerpt: article.meta_description ?? '',
    }, {
      headers:  { Authorization: `Bearer ${this.wpAuthToken}` },
      timeout:  20_000,
    });
  }

  collectMetrics() {
    return {
      articles_published: this.getCounter('articles_published'),
      backlinks_earned:   this.getCounter('backlinks_earned'),
      errors:             this.getCounter('errors'),
    };
  }

  getImprovementContext() {
    return {
      config:           this.config,
      target_niche:     this.targetNiche,
      articles_per_day: this.articlesPerDay,
      seed_keywords:    this.keywords.slice(0, 20),
    };
  }

  _resetDailyCounter() {
    const today = new Date().toDateString();
    if (today !== this._dayStart) {
      this._publishedToday = 0;
      this._dayStart       = today;
    }
  }
}
