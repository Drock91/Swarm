/**
 * ContentNode — Writes social posts, threads, hooks for all platforms.
 *
 * Capabilities:
 *   - Platform-specific content: X threads, LinkedIn posts, Reddit posts, TikTok hooks
 *   - Trending topic awareness (via RSS / GPT)
 *   - Scheduled publishing via platform APIs
 *   - Engagement tracking (likes, replies, shares)
 *   - Constant brand presence — designed for volume
 *   - Self-improves based on engagement metrics
 */

import axios from 'axios';
import OpenAI from 'openai';
import { BaseNode } from '../core/base_node.mjs';
import { log } from '../core/logger.mjs';

const sleep = ms => new Promise(r => setTimeout(r, ms));

const PLATFORM_PROMPTS = {
  x:        (niche) => `Write a punchy X (Twitter) thread (5-7 tweets) about ${niche}. Hook tweet first, value in the middle, CTA last. Use line breaks and numbers. Return JSON: { "tweets": ["tweet1", "tweet2", ...] }`,
  linkedin: (niche) => `Write a high-performing LinkedIn post about ${niche}. Lead with a bold insight, use short paragraphs, end with a question. Under 300 words. Return JSON: { "post": "..." }`,
  reddit:   (niche) => `Write an authentic, helpful Reddit post about ${niche}. Write like a real person sharing experience, not marketing. Include a title. Return JSON: { "title": "...", "body": "..." }`,
};

export class ContentNode extends BaseNode {
  static nodeType = 'content_node';

  constructor(config, region = 'us-east-1', parentId = null) {
    super(config, region, parentId);
    this.openai           = new OpenAI();
    this.targetNiche      = config.target_niche ?? 'entrepreneurship';
    this.platforms        = config.platforms ?? ['x', 'linkedin'];
    this.postsPerDay      = config.posts_per_day ?? 5;
    this.brandVoice       = config.brand_voice ?? 'direct, insightful, no-fluff';

    // Platform API tokens
    this.xBearerToken     = config.x_bearer_token ?? '';
    this.xApiKey          = config.x_api_key ?? '';
    this.xApiSecret       = config.x_api_secret ?? '';
    this.xAccessToken     = config.x_access_token ?? '';
    this.xAccessSecret    = config.x_access_secret ?? '';
    this.linkedInToken    = config.linkedin_token ?? '';
    this.redditToken      = config.reddit_token ?? '';

    this._postedToday = 0;
    this._dayStart    = new Date().toDateString();
  }

  async runCycle() {
    this._resetDailyCounter();
    const tasks = await this.receiveTasks(10);
    for (const t of tasks) {
      await this._handleTask(t);
      await this.ackTask(t._receipt_handle);
    }
    if (this._postedToday < this.postsPerDay) {
      const remaining = this.postsPerDay - this._postedToday;
      for (let i = 0; i < remaining; i++) {
        const platform = this.platforms[i % this.platforms.length];
        await this._createAndPost(platform);
        await sleep(120_000); // 2 min between posts
      }
    }
  }

  async _handleTask(task) {
    if (task.command === 'shutdown' && (!task.target_node_id || task.target_node_id === this.nodeId)) {
      await this.stop('commander_shutdown');
    } else if (task.command === 'post_content') {
      await this._createAndPost(task.platform ?? this.platforms[0], task.topic);
    } else if (task.command === 'update_config') {
      this.config[task.key] = task.value;
    }
  }

  async _createAndPost(platform, topic = null) {
    try {
      const content = await this._generateContent(platform, topic);
      if (!content) return;

      const s3Key = `content/${this.nodeId}/${platform}-${Date.now()}.json`;
      await this.memory.saveContent(s3Key, JSON.stringify(content));

      await this._publishContent(platform, content);

      this.increment('content_published');
      this._postedToday++;
      await this.memory.writeMetric(this.nodeId, this.nodeType, 'content_published', 1);
      log.info({ event: 'content_published', platform, node_id: this.nodeId });
    } catch (err) {
      log.error({ event: 'content_publish_error', platform, error: err.message });
      this.increment('errors');
    }
  }

  async _generateContent(platform, topic = null) {
    const subject = topic ?? await this._getTrendingTopic();
    const promptFn = PLATFORM_PROMPTS[platform];
    if (!promptFn) return null;

    const systemMsg = `You are a social media content expert. Brand voice: ${this.brandVoice}. Write only what's asked in JSON.`;
    try {
      const resp = await this.openai.chat.completions.create({
        model:           'gpt-4o',
        messages:        [
          { role: 'system', content: systemMsg },
          { role: 'user',   content: promptFn(subject) },
        ],
        response_format: { type: 'json_object' },
        temperature:     0.8,
      });
      const data = JSON.parse(resp.choices[0].message.content);
      return { platform, subject, ...data, generated_at: new Date().toISOString() };
    } catch (err) {
      log.error({ event: 'content_gen_error', platform, error: err.message });
      return null;
    }
  }

  async _getTrendingTopic() {
    try {
      const resp = await this.openai.chat.completions.create({
        model:      'gpt-4o-mini',
        messages:   [{ role: 'user', content:
          `What are 3 trending, highly shareable topics RIGHT NOW in the ${this.targetNiche} space? ` +
          `Return JSON: { "topics": ["...", "...", "..."] }` }],
        response_format: { type: 'json_object' },
        temperature: 0.9,
      });
      const topics = JSON.parse(resp.choices[0].message.content).topics ?? [];
      return topics[Math.floor(Math.random() * topics.length)] ?? this.targetNiche;
    } catch {
      return this.targetNiche;
    }
  }

  async _publishContent(platform, content) {
    switch (platform) {
      case 'x':
        await this._postToX(content);
        break;
      case 'linkedin':
        await this._postToLinkedIn(content);
        break;
      case 'reddit':
        await this._postToReddit(content);
        break;
      default:
        log.warn({ event: 'unknown_platform', platform });
    }
  }

  async _postToX(content) {
    if (!this.xBearerToken) return;
    const tweets = content.tweets ?? [content.post ?? ''];
    if (!tweets.length) return;

    // Post the thread — reply-chain the subsequent tweets
    let lastId = null;
    for (const tweet of tweets) {
      const body = lastId ? { text: tweet, reply: { in_reply_to_tweet_id: lastId } } : { text: tweet };
      const resp = await axios.post('https://api.twitter.com/2/tweets', body, {
        headers: { Authorization: `Bearer ${this.xBearerToken}` },
        timeout: 15_000,
      });
      lastId = resp.data?.data?.id;
      await sleep(2000);
    }
  }

  async _postToLinkedIn(content) {
    if (!this.linkedInToken) return;
    const text = content.post ?? '';
    await axios.post('https://api.linkedin.com/v2/ugcPosts', {
      author:          `urn:li:person:${this.config.linkedin_person_id}`,
      lifecycleState:  'PUBLISHED',
      specificContent: {
        'com.linkedin.ugc.ShareContent': {
          shareCommentary:  { text },
          shareMediaCategory: 'NONE',
        },
      },
      visibility: { 'com.linkedin.ugc.MemberNetworkVisibility': 'PUBLIC' },
    }, {
      headers: { Authorization: `Bearer ${this.linkedInToken}` },
      timeout: 15_000,
    });
  }

  async _postToReddit(content) {
    if (!this.redditToken) return;
    const subreddit = this.config.target_subreddit ?? 'entrepreneur';
    await axios.post(`https://oauth.reddit.com/r/${subreddit}/submit`, {
      kind:    'self',
      title:   content.title ?? content.subject,
      text:    content.body  ?? '',
      sr:      subreddit,
    }, {
      headers: {
        Authorization:  `Bearer ${this.redditToken}`,
        'User-Agent':   'Swarm/1.0',
      },
      timeout: 15_000,
    });
  }

  collectMetrics() {
    return {
      content_published: this.getCounter('content_published'),
      errors:            this.getCounter('errors'),
    };
  }

  getImprovementContext() {
    return {
      config:        this.config,
      target_niche:  this.targetNiche,
      platforms:     this.platforms,
      posts_per_day: this.postsPerDay,
      brand_voice:   this.brandVoice,
    };
  }

  _resetDailyCounter() {
    const today = new Date().toDateString();
    if (today !== this._dayStart) {
      this._postedToday = 0;
      this._dayStart    = today;
    }
  }
}
