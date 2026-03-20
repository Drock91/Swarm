/**
 * Simple structured logger using pino.
 * Import { log } from './logger.mjs' everywhere.
 */

import pino from 'pino';

export const log = pino({
  level: process.env.LOG_LEVEL ?? 'info',
  transport: process.env.NODE_ENV !== 'production'
    ? { target: 'pino-pretty', options: { colorize: true, translateTime: 'SYS:HH:MM:ss' } }
    : undefined,
});
