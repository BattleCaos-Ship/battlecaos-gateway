import Redis from 'ioredis';
import { log } from './logger.js';

// ── Cliente Redis RESILIENTE (primario + respaldo) ────────────────────────────
// Si REDIS_FALLBACK_URL está definida, createRedis() devuelve un cliente que:
//   • ESCRIBE en AMBOS Redis (doble escritura) → el respaldo siempre tiene el mismo
//     estado, así una caída del primario NO pierde las partidas en curso.
//   • LEE del primario; si el primario está caído, lee del respaldo.
//   • Vigila ambos nodos con PING y vuelve al primario al recuperarse.
// Si NO hay REDIS_FALLBACK_URL, devuelve un cliente único (comportamiento original).
//
// Nota de consistencia: el `game` reescribe el estado COMPLETO de la sala en cada acción,
// así que un nodo que se recupere converge solo en la siguiente jugada. Los contadores
// relativos (incrby de energía) pueden divergir durante una caída; se acepta como menor.

const OPTS_SIMPLE     = { lazyConnect: true, maxRetriesPerRequest: 3 };
const OPTS_RESILIENTE = { lazyConnect: true, maxRetriesPerRequest: 2, enableOfflineQueue: false, commandTimeout: 2000 };

// Comandos de LECTURA (no mutan estado): se sirven del primario, con caída al respaldo.
export const READ_CMDS = new Set([
  'get', 'mget', 'strlen', 'exists', 'ttl', 'pttl', 'type', 'getrange',
  'lrange', 'llen', 'lindex', 'hget', 'hgetall', 'hmget', 'hkeys', 'hvals', 'hlen',
  'scard', 'smembers', 'sismember', 'zrange', 'zcard', 'zscore', 'zrank',
  'ping', 'keys', 'dbsize', 'info',
]);
// Comandos de ESCRITURA (mutan estado): van a AMBOS Redis.
export const WRITE_CMDS = new Set([
  'set', 'setex', 'setnx', 'psetex', 'getset', 'append', 'del', 'unlink',
  'expire', 'pexpire', 'expireat', 'persist', 'rename',
  'incr', 'incrby', 'incrbyfloat', 'decr', 'decrby',
  'sadd', 'srem', 'spop', 'lpush', 'rpush', 'lpop', 'rpop', 'ltrim', 'lrem', 'lset',
  'hset', 'hsetnx', 'hmset', 'hincrby', 'hincrbyfloat', 'hdel',
  'zadd', 'zrem', 'zincrby',
]);

export function esErrorDeConexion(err) {
  if (!err) return false;
  if (err.name === 'MaxRetriesPerRequestError') return true;
  const m = String(err.message || err);
  return /ECONNREFUSED|ETIMEDOUT|ENOTFOUND|EPIPE|EHOSTUNREACH|Connection is closed|Command timed out|Stream isn't writeable|Reached the max retries|failed to refresh|Connection is broken/i.test(m);
}

// Envuelve dos clientes ioredis (primario, respaldo) en un cliente con doble escritura y
// failover. Exportado para poder testear la lógica con clientes simulados.
export function makeResilient(primary, secondary, { healthMs = 3000 } = {}) {
  const salud = { p: true, s: true };

  const marcar = (nodo, ok, err) => {
    if (salud[nodo] === ok) return;
    salud[nodo] = ok;
    const cual = nodo === 'p' ? 'PRIMARIO' : 'RESPALDO';
    if (ok) log.info(`redis ${cual} recuperado.`);
    else    log.warn(`redis ${cual} caído → operando con el otro nodo. (${err?.message ?? err})`);
  };

  // Health-check periódico: reincorpora un nodo caído en cuanto responde a PING.
  const timer = setInterval(async () => {
    try { await primary.ping();   marcar('p', true); } catch { /* sigue caído */ }
    try { await secondary.ping(); marcar('s', true); } catch { /* sigue caído */ }
  }, healthMs);
  timer.unref?.();

  // LECTURA: nodo preferido primero (primario si está sano); ante error de conexión, el otro.
  async function read(cmd, args) {
    const orden = salud.p ? [['p', primary], ['s', secondary]] : [['s', secondary], ['p', primary]];
    let ultimoErr;
    for (const [nodo, cli] of orden) {
      try { const r = await cli[cmd](...args); marcar(nodo, true); return r; }
      catch (err) {
        ultimoErr = err;
        if (esErrorDeConexion(err)) marcar(nodo, false, err);
        else throw err; // error real (no de conexión) → no reintentar en el otro nodo
      }
    }
    throw ultimoErr; // ambos nodos caídos
  }

  // Replica un comando al secundario en SEGUNDO PLANO (no bloquea la ruta caliente).
  function replicar(cmd, args) {
    Promise.resolve(secondary[cmd](...args))
      .then(() => marcar('s', true))
      .catch((e) => { if (esErrorDeConexion(e)) marcar('s', false, e); });
  }

  // ESCRITURA: se CONFIRMA con el primario (ruta rápida) y se REPLICA al secundario en segundo
  // plano → durabilidad sin pagar la latencia del nodo remoto en cada interacción. Si el primario
  // está caído, se escribe en el secundario de forma síncrona (failover). Se replica una sola vez
  // (nunca en ambos caminos) para no duplicar comandos relativos como incrby.
  async function write(cmd, args) {
    if (salud.p) {
      try {
        const v = await primary[cmd](...args);
        marcar('p', true);
        if (salud.s) replicar(cmd, args); // réplica en segundo plano al secundario
        return v;
      } catch (err) {
        if (!esErrorDeConexion(err)) throw err;
        marcar('p', false, err); // primario caído → cae al secundario síncrono abajo
      }
    }
    const v = await secondary[cmd](...args);
    marcar('s', true);
    return v;
  }

  // pipeline(): encadena en AMBOS pipelines y ejecuta los dos en .exec().
  function pipeline() {
    const p = primary.pipeline();
    const s = secondary.pipeline();
    const wrap = new Proxy(function () {}, {
      get(_t, prop) {
        if (prop === 'exec') {
          // Igual que write(): confirma con el primario, replica el pipeline al secundario en
          // segundo plano. Así el broadcastState no espera al nodo remoto.
          return async () => {
            if (salud.p) {
              try {
                const r = await p.exec();
                marcar('p', true);
                if (salud.s) s.exec().then(() => marcar('s', true))
                  .catch((e) => { if (esErrorDeConexion(e)) marcar('s', false, e); });
                return r;
              } catch (err) {
                if (!esErrorDeConexion(err)) throw err;
                marcar('p', false, err);
              }
            }
            const r = await s.exec();
            marcar('s', true);
            return r;
          };
        }
        return (...args) => { p[prop]?.(...args); s[prop]?.(...args); return wrap; };
      },
    });
    return wrap;
  }

  return new Proxy({}, {
    get(_t, prop) {
      if (prop === 'pipeline' || prop === 'multi') return pipeline;
      if (prop === 'connect') {
        return async () => {
          const r = await Promise.allSettled([primary.connect(), secondary.connect()]);
          if (r[0].status === 'rejected') marcar('p', false, r[0].reason);
          if (r[1].status === 'rejected') marcar('s', false, r[1].reason);
        };
      }
      if (prop === 'quit' || prop === 'disconnect') {
        return async () => { clearInterval(timer); await Promise.allSettled([primary[prop]?.(), secondary[prop]?.()]); };
      }
      if (typeof prop === 'string') {
        if (WRITE_CMDS.has(prop)) return (...args) => write(prop, args);
        if (READ_CMDS.has(prop))  return (...args) => read(prop, args);
      }
      // Eventos (.on/.once), status, options, etc. → delegar al primario.
      const v = primary[prop];
      return typeof v === 'function' ? v.bind(primary) : v;
    },
  });
}

export function createRedis() {
  const fallbackUrl = process.env.REDIS_FALLBACK_URL;

  if (!fallbackUrl) {
    // Sin respaldo configurado → cliente único (comportamiento original, sin cambios).
    const client = new Redis(process.env.REDIS_URL, OPTS_SIMPLE);
    client.on('connect', () => log.info('redis conectado'));
    client.on('error',   (err) => log.error('redis error:', err.message));
    return client;
  }

  // Modo resiliente: primario (REDIS_URL) + respaldo (REDIS_FALLBACK_URL) con doble escritura.
  const primary   = new Redis(process.env.REDIS_URL, OPTS_RESILIENTE);
  const secondary = new Redis(fallbackUrl, OPTS_RESILIENTE);
  primary.on('connect',   () => log.info('redis conectado (primario)'));
  primary.on('error',     (err) => log.error('redis error (primario):', err.message));
  secondary.on('connect', () => log.info('redis conectado (respaldo)'));
  secondary.on('error',   (err) => log.error('redis error (respaldo):', err.message));
  log.info('redis en modo RESILIENTE (primario + respaldo, doble escritura)');
  return makeResilient(primary, secondary);
}
