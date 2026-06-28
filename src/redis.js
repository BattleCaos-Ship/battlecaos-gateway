import Redis from 'ioredis';
import { log } from './logger.js';

export function createRedis() {
  const client = new Redis(process.env.REDIS_URL, { lazyConnect: true, maxRetriesPerRequest: 3 });

  client.on('connect', () => log.info('redis conectado'));
  client.on('error',   (err) => log.error('redis error', err.message));

  return client;
}
