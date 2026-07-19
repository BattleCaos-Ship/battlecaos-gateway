import 'dotenv/config';
import { randomUUID } from 'node:crypto';
import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import helmet from 'helmet';
import jwt from 'jsonwebtoken';
import { createRedis } from './redis.js';
import { conLockSala } from './lock.js';
import { crearBackoff } from './backoff.js';
import { producer, createConsumer } from './kafka.js';
import { log } from './logger.js';
import { ROUTES, buildMessage } from './domain/router.js';
import {
  register, httpDuration, socketConnectionsTotal, eventsRoutedTotal,
  rateLimitExceededTotal, reconnectionsTotal, kafkaConsumerCrashTotal, activeSocketsGauge,
  kafkaConsumerUp,
} from './metrics.js';
import { httpMetricsMiddleware } from './httpMetricsMiddleware.js';

const PORT   = process.env.GATEWAY_PORT ?? 3000;
const ORIGIN = process.env.CLIENT_ORIGIN ?? '*';

// ID único por instancia de gateway. Cada instancia consume gw.broadcast/evt.* en su PROPIO
// consumer group (ver más abajo) para recibir TODOS los mensajes (fan-out) y entregarlos a sus
// sockets locales. Así se pueden correr N gateways tras un balanceador. En despliegue conviene
// fijar INSTANCE_ID (p.ej. el nombre del pod) para no crear un grupo nuevo en cada reinicio.
// En Azure Container Apps cada réplica trae CONTAINER_APP_REPLICA_NAME — usarlo como ID
// estable evita que cada reinicio cree un consumer group nuevo en Kafka.
const INSTANCE_ID = process.env.INSTANCE_ID
  ?? process.env.CONTAINER_APP_REPLICA_NAME
  ?? randomUUID();

const app    = express();
const server = createServer(app);
const io     = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
});

app.use(helmet({ crossOriginResourcePolicy: false }));
app.use(cors({ origin: '*' }));
app.use(express.json());
app.use(httpMetricsMiddleware(httpDuration));

app.get('/metrics', async (_req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

// Redis — solo estado: rate limiting, reconexión, KPIs
const redis = createRedis();
await redis.connect();

// Índice inverso jugador→sala: permite reencontrar la partida de un jugador en O(1) al
// reconectar, en vez de escanear TODO el keyspace con KEYS 'sala:*' (O(N), bloqueante) en
// CADA conexión. Lo escribe el servicio `room` al unirse; aquí solo se lee y se refresca.
const IDX_TTL_SEG = 60 * 60 * 12; // 12 h — cubre cualquier partida; se refresca al reconectar.
const idxJugador = (playerId) => `jugador:${playerId}:sala`;
const SALA_TTL_SEG = 60 * 60 * 6; // 6 h — mismo TTL que aplican room/game; se re-aplica al reconectar.

// Kafka — mensajería entre servicios
await producer.connect();
// Consumer group ÚNICO por instancia: los broadcasts (gw.broadcast/evt.timer/evt.game) son
// mensajes de fan-out — cada gateway debe recibir TODOS y entregarlos a sus sockets locales.
// Con un grupo compartido, Kafka repartiría las particiones y cada broadcast llegaría a UNA
// sola instancia → los clientes conectados a otra no lo recibirían. El grupo por instancia
// hace que todos reciban todo (el balanceador solo necesita sticky sessions).
const consumer = createConsumer(`gateway-group-${INSTANCE_ID}`);

// Salud del consumer de Kafka del gateway (mismo patrón que trackConsumer en los servicios
// internos). Un consumer colgado deja de entregar broadcasts → la partida se congela para
// los clientes conectados a esta réplica, aunque Redis siga respondiendo. Sin esto, /health
// daba 200 igual y la liveness probe NUNCA reiniciaba la réplica (el hueco del hallazgo v2).
let consumerVivo    = false; // ¿está procesando ahora mismo?
let consumerUnidoAlguna = false; // ¿llegó a unirse al grupo alguna vez? (evita 503 en el arranque)
function marcarConsumer(vivo) {
  consumerVivo = vivo;
  if (vivo) consumerUnidoAlguna = true;
  kafkaConsumerUp.set(vivo ? 1 : 0);
}
// Backoff exponencial + jitter para reconectar (antes: 5s fijos → thundering herd al volver Kafka).
const reconexionKafka = crearBackoff({ baseMs: 1000, maxMs: 30000 });
consumer.on(consumer.events.GROUP_JOIN, () => { marcarConsumer(true); reconexionKafka.reset(); });
consumer.on(consumer.events.CONNECT,    () => marcarConsumer(true));
consumer.on(consumer.events.STOP,       () => marcarConsumer(false));
consumer.on(consumer.events.DISCONNECT, () => marcarConsumer(false));

// DOMF1302 — /health: ping Redis real + salud del consumer de Kafka. Devuelve 503 (→ la
// liveness probe reinicia) si Redis no responde O si el consumer se unió alguna vez pero
// ahora está caído. Durante el arranque (antes del primer GROUP_JOIN) no penaliza.
app.get('/health', async (_req, res) => {
  try {
    const ping = await redis.ping();
    const consumerCaido = consumerUnidoAlguna && !consumerVivo;
    const estado = {
      service:   'gateway',
      status:    consumerCaido ? 'error' : 'ok',
      redis:     ping === 'PONG' ? 'ok' : 'degraded',
      kafka:     consumerUnidoAlguna ? (consumerVivo ? 'ok' : 'down') : 'starting',
      uptime:    process.uptime(),
      timestamp: Date.now(),
    };
    res.status(consumerCaido ? 503 : 200).json(estado);
  } catch (err) {
    res.status(503).json({ service: 'gateway', status: 'error', redis: 'down', error: err.message });
  }
});

// UIUXF1101 — /kpis desde Redis State (escritos por Observability Service)
app.get('/kpis', async (_req, res) => {
  try {
    const today   = new Date().toISOString().split('T')[0];
    const raw     = await redis.hgetall(`stats:kpi:${today}`) ?? {};
    const started = parseInt(raw.games_started ?? 0);
    const ended   = parseInt(raw.games_ended   ?? 0);
    const disc    = parseInt(raw.disconnections ?? 0);
    const recon   = parseInt(raw.reconnections  ?? 0);
    const pico    = await redis.get('metrics:pico:salas') ?? 0;

    // Latencia P95 de disparos (nearest-rank) desde la lista escrita por observability.
    const lat   = (await redis.lrange('metrics:latencia:shots', 0, 999)).map(Number).filter((n) => !isNaN(n));
    let p95 = 'N/A';
    if (lat.length) {
      lat.sort((a, b) => a - b);
      p95 = lat[Math.min(lat.length - 1, Math.ceil(0.95 * lat.length) - 1)] + ' ms';
    }

    res.json({
      tasa_completacion: started ? ((ended / started) * 100).toFixed(1) + '%' : 'N/A',
      tasa_reconexion:   disc    ? ((recon  / disc)   * 100).toFixed(1) + '%' : 'N/A',
      pico_salas:        pico,
      latencia_p95:      p95,
      date:              today,
    });
  } catch (err) {
    res.status(503).json({ error: err.message });
  }
});

// DOMF102 — Verificación JWT antes de cualquier evento
io.use((socket, next) => {
  const token = socket.handshake.auth?.token ?? socket.handshake.query?.token;
  if (!token) return next(new Error('sin_token'));
  try {
    socket.user = jwt.verify(token, process.env.JWT_SECRET);
    next();
  } catch {
    next(new Error('token_invalido'));
  }
});

// DOMF002 — Rate limiting con Redis State (ventana 1s, máx 20 eventos/IP)
io.use(async (socket, next) => {
  const key   = `ratelimit:${socket.handshake.address}`;
  const count = await redis.incr(key);
  if (count === 1) await redis.expire(key, 1);
  if (count > 20) {
    rateLimitExceededTotal.inc();
    log.warn('rate_limit_exceeded', socket.handshake.address);
    return next(new Error('rate_limit_exceeded'));
  }
  next();
});

// DOMF301 — Consumir Kafka y hacer broadcast a clientes Socket.io
async function startConsumer() {
  try {
    await consumer.connect();
    await consumer.subscribe({ topics: ['gw.broadcast', 'evt.timer', 'evt.game'], fromBeginning: false });

    consumer.on(consumer.events.CRASH, async () => {
      kafkaConsumerCrashTotal.inc();
      marcarConsumer(false); // /health pasa a 503 → la liveness probe reinicia si no reconecta
      const delay = reconexionKafka.siguiente();
      log.warn(`kafka consumer crasheó — reconectando en ${delay}ms (intento ${reconexionKafka.intentos})...`);
      setTimeout(async () => {
        try {
          await consumer.disconnect();
          await startConsumer();
        } catch (err) {
          log.error('error al reconectar consumer —', err.message);
        }
      }, delay);
    });

    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        try {
          const raw = message.value.toString();
          if (topic === 'gw.broadcast') {
            const { roomId, event, payload } = JSON.parse(raw);
            if (event === 'room:join-socket-room') {
              const socket = io.sockets.sockets.get(roomId);
              socket?.join(payload.codigo);
              log.info(`socket ${roomId} joined room ${payload.codigo}`);
              return;
            }
            io.to(roomId).emit(event, payload);
          } else if (topic === 'evt.timer') {
            const msg = JSON.parse(raw);
            if (msg.type === 'TimerTick') io.to(msg.data.codigo).emit('timer:tick', msg.data);
          } else if (topic === 'evt.game') {
            const msg = JSON.parse(raw);
            if (msg.type === 'PhaseChanged') io.to(msg.data.codigo).emit('phase:changed', msg.data);
          }
        } catch (err) {
          log.error('kafka message parse fail —', err.message);
        }
      },
    });
    log.info('kafka consumer listo');
  } catch (err) {
    const delay = reconexionKafka.siguiente();
    log.warn(`kafka consumer no disponible, reintentando en ${delay}ms — ${err.message}`);
    setTimeout(startConsumer, delay);
  }
}

startConsumer();

io.on('connection', async (socket) => {
  log.info('cliente conectado:', socket.id);
  socketConnectionsTotal.inc();
  activeSocketsGauge.inc();

  // DOMF401 — Reconexión: buscar sala activa en Redis State
  const playerId = socket.user.sub;

  // Los servicios (game) dirigen errores/broadcasts personales a io.to(playerId).
  // Socket.io solo une cada socket a un room con su socket.id por defecto, así que
  // unimos explícitamente al room nombrado por el playerId para que esos mensajes lleguen.
  socket.join(playerId);

  try {
    // O(1): el índice inverso nos da directamente la sala del jugador (antes: KEYS 'sala:*').
    const codigo = await redis.get(idxJugador(playerId));
    // El read-modify-write de la sala en la reconexión corre bajo el MISMO lock por sala
    // que usa el game service → nunca pisa (ni es pisado por) un disparo que ocurra a la
    // vez. Cierra la carrera cross-service del hallazgo #2.
    if (codigo) await conLockSala(redis, codigo, async () => {
      const raw     = await redis.get(`sala:${codigo}`);
      const sala    = raw ? JSON.parse(raw) : null;
      const jugador = sala?.jugadores?.find((j) => j.id === playerId);

      if (sala && jugador && sala.fase !== 'FIN') {
        socket.join(sala.codigo);
        jugador.socketId  = socket.id;
        jugador.conectado = true;
        await redis.set(`sala:${sala.codigo}`, JSON.stringify(sala), 'EX', SALA_TTL_SEG); // re-aplica TTL
        await redis.set(idxJugador(playerId), sala.codigo, 'EX', IDX_TTL_SEG); // refresca TTL
        await producer.send({
          topic:    'evt.room',
          messages: [{ key: sala.codigo, value: JSON.stringify({
            type:      'PlayerReconnected',
            source:    'gateway',
            timestamp: Date.now(),
            data:      { codigo: sala.codigo, playerId },
          })}],
        });
        socket.emit('game:state', sala);
        reconnectionsTotal.inc();
        log.info(`reconexión: ${playerId} → sala ${sala.codigo}`);
      } else {
        // Índice obsoleto (partida terminada, sala borrada, o el jugador ya no está): limpiarlo
        // para que no vuelva a intentar reincorporar a una partida inexistente.
        await redis.del(idxJugador(playerId));
      }
    });
  } catch (err) {
    log.error('error en reconexión:', err.message);
  }

  // Re-sincronización a demanda: el cliente pide el estado actual de SU sala (p.ej.
  // GamePage montó sin estado porque el game:state de reconexión llegó antes de que
  // sus listeners estuvieran adjuntos — sin esto quedaba "cargando" hasta el
  // siguiente broadcast de la partida).
  socket.on('game:sync', async () => {
    try {
      const codigo = await redis.get(idxJugador(playerId));
      const raw    = codigo ? await redis.get(`sala:${codigo}`) : null;
      const sala   = raw ? JSON.parse(raw) : null;
      // Una partida TERMINADA (FIN) o inexistente no debe reincorporar al jugador: se
      // limpia el índice obsoleto para que quede libre de elegir modo / crear otra sala.
      if (!sala || sala.fase === 'FIN') {
        if (codigo) await redis.del(idxJugador(playerId));
        return;
      }
      socket.emit('game:state', sala);
    } catch (err) {
      log.error('error en game:sync:', err.message);
    }
  });

  // Modo ESPECTADOR: cualquiera con el código puede VER una partida en curso (solo
  // lectura). Se une al room de Socket.io (recibe los game:state sanitizados que
  // emite el game service) y recibe el estado COMPLETO (con `tableros`): el
  // espectador ve las posiciones de las flotas de AMBOS equipos con sus sprites.
  socket.on('room:espectar', async ({ codigo } = {}) => {
    try {
      const raw = codigo ? await redis.get(`sala:${String(codigo)}`) : null;
      if (!raw) return socket.emit('room:error', { error: 'sala_no_existe' });
      const sala = JSON.parse(raw);
      if (sala.fase === 'LOBBY') return socket.emit('room:error', { error: 'partida_no_iniciada' });

      socket.join(sala.codigo);
      socket.emit('game:state', sala);
      log.info(`espectador ${playerId} → sala ${sala.codigo}`);
    } catch (err) {
      log.error('error en room:espectar:', err.message);
    }
  });

  socket.on('room:dejar-espectar', ({ codigo } = {}) => {
    if (codigo) socket.leave(String(codigo));
  });

  // Preview de colocación en VIVO (2v2): mientras un jugador acomoda sus barcos (antes
  // de confirmar), su COMPAÑERO ve cada barco donde lo va poniendo. Reenviado VÍA KAFKA
  // (gw.broadcast) por la misma razón que voice:signal: con 2+ réplicas del gateway, el
  // compañero puede estar en OTRA réplica y un io.to() local nunca le llegaría.
  socket.on('colocacion:preview', async ({ codigo, ships } = {}) => {
    try {
      if (!codigo || !Array.isArray(ships) || ships.length > 8) return;
      const raw = await redis.get(`sala:${String(codigo)}`);
      if (!raw) return;
      const sala = JSON.parse(raw);
      const yo = sala.jugadores?.find((j) => j.id === playerId);
      if (!yo) return;
      for (const j of sala.jugadores) {
        if (j.id === playerId || j.equipo !== yo.equipo || j.esBot) continue;
        await producer.send({
          topic:    'gw.broadcast',
          messages: [{ key: String(j.id), value: JSON.stringify({
            roomId: j.id, event: 'equipo:preview', payload: { de: playerId, ships },
          })}],
        });
      }
    } catch (err) {
      log.error('error en colocacion:preview:', err.message);
    }
  });

  // Señalización WebRTC de VOZ (SDP/ICE) — relay efímero validado (misma sala) y
  // reenviado VÍA KAFKA (gw.broadcast), NO con io.to() local. Con el gateway en
  // ACTIVO/ACTIVO (2+ réplicas), cada jugador puede estar conectado a una réplica
  // DISTINTA: un io.to(to) local solo alcanza los sockets de ESTA réplica → la
  // señalización nunca cruzaba y la voz quedaba en "conectando" para siempre.
  // gw.broadcast hace fan-out a TODAS las réplicas y la dueña del socket entrega.
  socket.on('voice:signal', async ({ codigo, to, data } = {}) => {
    try {
      if (!codigo || !to || data == null) return;
      const raw = await redis.get(`sala:${String(codigo)}`);
      if (!raw) return;
      const sala = JSON.parse(raw);
      const yo      = sala.jugadores?.some((j) => j.id === playerId);
      const destino = sala.jugadores?.some((j) => j.id === to);
      if (!yo || !destino) return;
      await producer.send({
        topic:    'gw.broadcast',
        messages: [{ key: String(to), value: JSON.stringify({
          roomId: to, event: 'voice:signal', payload: { from: playerId, data },
        })}],
      });
    } catch (err) {
      log.error('error en voice:signal:', err.message);
    }
  });

  // DOMF002 — Enrutar eventos del cliente a topics Kafka, con RATE LIMIT POR EVENTO.
  // El io.use() de arriba solo limita HANDSHAKES (1 vez por conexión); esto limita el
  // VOLUMEN de eventos de un socket ya conectado (antivector de DoS y anti-flood). La
  // ventana es por PLAYER (socket.user.sub), no por IP: varios jugadores tras el mismo
  // NAT (sala de clase) no se estorban entre sí.
  const EVENT_LIMIT   = 30;  // eventos por ventana
  const EVENT_WINDOW  = 1;   // segundos
  for (const [event, topic] of Object.entries(ROUTES)) {
    socket.on(event, async (payload = {}) => {
      try {
        const key   = `ratelimit:evt:${socket.user.sub}`;
        const count = await redis.incr(key);
        if (count === 1) await redis.expire(key, EVENT_WINDOW);
        if (count > EVENT_LIMIT) {
          rateLimitExceededTotal.inc();
          socket.emit('rate_limited', { event });
          return; // se descarta el evento; el cliente legítimo casi nunca llega aquí
        }
      } catch (err) {
        log.error('rate limit check falló —', err.message); // fail-open: no bloquear por un fallo de Redis
      }
      const message = buildMessage(event, socket.id, socket.user.sub, payload);
      producer.send({
        topic,
        messages: [{ key: payload.codigo ?? socket.id, value: JSON.stringify(message) }],
      }).catch((err) => log.error('kafka send error —', err.message));
      eventsRoutedTotal.inc({ event });
      log.info(`${event} → ${topic}`);
    });
  }

  // DOMF002 — Notificar desconexión al Room Service vía Kafka
  socket.on('disconnect', () => {
    activeSocketsGauge.dec();
    log.info('cliente desconectado:', socket.id);
    producer.send({
      topic:    'cmd.room',
      messages: [{ value: JSON.stringify({
        type:      'PlayerDisconnected',
        source:    'gateway',
        timestamp: Date.now(),
        data:      { socketId: socket.id, playerId: socket.user.sub },
      })}],
    }).catch((err) => log.error('kafka disconnect error —', err.message));
  });
});

server.listen(PORT, () => log.info(`gateway :${PORT} (instancia ${INSTANCE_ID})`));
