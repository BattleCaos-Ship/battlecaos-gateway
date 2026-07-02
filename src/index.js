import 'dotenv/config';
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

const PORT   = process.env.GATEWAY_PORT ?? 3000;
const ORIGIN = process.env.CLIENT_ORIGIN ?? '*';

const app    = express();
const server = createServer(app);
const io     = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
});

app.use(helmet({ crossOriginResourcePolicy: false }));
app.use(cors({ origin: '*' }));
app.use(express.json());

// Redis — solo estado: rate limiting, reconexión, KPIs
const redis = createRedis();
await redis.connect();

// Kafka — mensajería entre servicios
await producer.connect();
const consumer = createConsumer('gateway-group');

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
    res.json({
      tasa_completacion: started ? ((ended / started) * 100).toFixed(1) + '%' : 'N/A',
      tasa_reconexion:   disc    ? ((recon  / disc)   * 100).toFixed(1) + '%' : 'N/A',
      pico_salas:        pico,
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

  // DOMF401 — Reconexión: buscar sala activa en Redis State
  const playerId = socket.user.sub;
  try {
    const keys = await redis.keys('sala:*');
    for (const key of keys) {
      if (key.split(':').length !== 2) continue;
      const raw = await redis.get(key);
      if (!raw) continue;
      const sala    = JSON.parse(raw);
      const jugador = sala?.jugadores?.find((j) => j.id === playerId);
      if (jugador && sala.fase !== 'FIN') {
        socket.join(sala.codigo);
        jugador.socketId  = socket.id;
        jugador.conectado = true;
        await redis.set(key, JSON.stringify(sala));
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
        log.info(`reconexión: ${playerId} → sala ${sala.codigo}`);
        break;
      }
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
      log.info(`${event} → ${topic}`);
    });
  }

  // DOMF002 — Notificar desconexión al Room Service vía Kafka
  socket.on('disconnect', () => {
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

server.listen(PORT, () => log.info(`:${PORT}`));
