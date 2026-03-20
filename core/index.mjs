/**
 * SWARM core — barrel export
 */

export { SharedMemory }        from './shared_memory.mjs';
export { BaseNode }            from './base_node.mjs';
export { CommanderAgent }      from './commander.mjs';
export { SelfImprovementEngine } from './self_improvement.mjs';
export { SwarmIntelligence }   from './swarm_intelligence.mjs';
export { log }                 from './logger.mjs';
export {
  loadProfile,
  emailNodeConfig,
  scraperNodeConfig,
  seoNodeConfig,
  analyticsNodeConfig,
  bootstrapGoals,
  mergeProfileConfig,
}                              from './profile.mjs';
