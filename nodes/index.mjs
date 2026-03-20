/**
 * nodes — barrel export + factory
 */

export { EmailNode }     from './email_node.mjs';
export { SEONode }       from './seo_node.mjs';
export { ScraperNode }   from './scraper_node.mjs';
export { AnalyticsNode } from './analytics_node.mjs';

import { EmailNode }     from './email_node.mjs';
import { SEONode }       from './seo_node.mjs';
import { ScraperNode }   from './scraper_node.mjs';
import { AnalyticsNode } from './analytics_node.mjs';

export const NODE_REGISTRY = {
  email_node:     EmailNode,
  seo_node:       SEONode,
  scraper_node:   ScraperNode,
  analytics_node: AnalyticsNode,
};

/**
 * Factory — create a node instance by type string.
 * @param {string} nodeType
 * @param {object} config
 * @param {string} [region]
 * @returns {import('../core/base_node.mjs').BaseNode}
 */
export function createNode(nodeType, config, region = 'us-east-1') {
  const Cls = NODE_REGISTRY[nodeType];
  if (!Cls) throw new Error(`Unknown node type: "${nodeType}". Valid: ${Object.keys(NODE_REGISTRY).join(', ')}`);
  return new Cls(config, region);
}
