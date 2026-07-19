import { randomUUID } from 'node:crypto';

// Lock distribuido por sala (SET NX PX) para serializar el read-modify-write de
// `sala:{codigo}` ENTRE procesos/servicios. El particionado de Kafka por `codigo` ya
// serializa los eventos de una sala DENTRO del game service; este lock cubre la carrera
// que ese particionado NO cubre: el gateway (3 réplicas) escribiendo la misma sala en la
// reconexión a la vez que el game resuelve un disparo. Es defensa explícita, no confiar
// en el invariante invisible del keying.
//
// `redisLike` solo necesita: set(key,val,'NX','PX',ms) → 'OK'|null, get(key), del(key).
export async function conLockSala(redisLike, codigo, fn, opts = {}) {
  const { ttlMs = 3000, reintentos = 25, esperaMs = 20 } = opts;
  const lockKey = `lock:sala:${codigo}`;
  const token   = randomUUID();
  const sleep   = (ms) => new Promise((r) => setTimeout(r, ms));

  for (let i = 0; i < reintentos; i++) {
    let adquirido = null;
    try { adquirido = await redisLike.set(lockKey, token, 'NX', 'PX', ttlMs); }
    catch { adquirido = null; } // fallo de Redis al pedir el lock → se reintenta

    if (adquirido) {
      try {
        return await fn();
      } finally {
        // Liberar SOLO si el token sigue siendo el nuestro (si el TTL ya expiró y otro
        // tomó el lock, no se lo quitamos). Best-effort: un fallo aquí no rompe nada.
        try {
          const actual = await redisLike.get(lockKey);
          if (actual === token) await redisLike.del(lockKey);
        } catch { /* el TTL lo limpiará */ }
      }
    }
    await sleep(esperaMs);
  }

  // No se pudo adquirir el lock tras todos los reintentos: mejor procesar el evento
  // (sin lock) que perderlo. La ventana de carrera vuelve a existir solo en este caso raro.
  return fn();
}
