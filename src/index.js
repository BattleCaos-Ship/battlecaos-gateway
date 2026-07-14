import 'dotenv/config';
import { randomUUID } from 'node:crypto';
import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import helmet from 'helmet';
import jwt from 'jsonwebtoken';
import { createRedis } from './redis.js';
import { producer, createConsumer } from './kafka.js';
import { log } from './logger.js';
import { ROUTES, buildMessage } from './domain/router.js';
import {
  register, httpDuration, socketConnectionsTotal, eventsRoutedTotal,
  rateLimitExceededTotal, reconnectionsTotal, kafkaConsumerCrashTotal, activeSocketsGauge,
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

// DOMF1302 — /health con ping Redis real
app.get('/health', async (_req, res) => {
  try {
    const ping = await redis.ping();
    res.json({
      service:   'gateway',
      status:    'ok',
      redis:     ping === 'PONG' ? 'ok' : 'degraded',
      uptime:    process.uptime(),
      timestamp: Date.now(),
    });
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
      log.warn('kafka consumer crasheó — reconectando en 5s...');
      setTimeout(async () => {
        try {
          await consumer.disconnect();
          await startConsumer();
        } catch (err) {
          log.error('error al reconectar consumer —', err.message);
        }
      }, 5000);
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
    log.warn(`kafka consumer no disponible, reintentando en 5s — ${err.message}`);
    setTimeout(startConsumer, 5000);
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
    const codigo  = await redis.get(idxJugador(playerId));
    const raw     = codigo ? await redis.get(`sala:${codigo}`) : null;
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
    } else if (codigo) {
      // Índice obsoleto (partida terminada, sala borrada, o el jugador ya no está): limpiarlo
      // para que no vuelva a intentar reincorporar a una partida inexistente.
      await redis.del(idxJugador(playerId));
    }
  } catch (err) {
    log.error('error en reconexión:', err.message);
  }

  // DOMF002 — Enrutar eventos del cliente a topics Kafka
  for (const [event, topic] of Object.entries(ROUTES)) {
    socket.on(event, (payload = {}) => {
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
