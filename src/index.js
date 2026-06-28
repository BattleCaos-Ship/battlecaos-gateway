import 'dotenv/config';
import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import helmet from 'helmet';
import jwt from 'jsonwebtoken';
import { createRedis } from './redis.js';
import { log } from './logger.js';

const PORT   = process.env.GATEWAY_PORT ?? 3000;
const ORIGIN = process.env.CLIENT_ORIGIN ?? 'http://localhost:5173';

const ROUTES = {
  'room:create':          'svc:room',
  'room:join':            'svc:room',
  'disparo:realizar':     'svc:game',
  'salva:disparo':        'svc:game',
  'poder:usar':           'svc:game',
  'colocacion:set':       'svc:game',
  'contramedida:activar': 'svc:game',
  'chat:mensaje':         'svc:chat',
};

const app    = express();
const server = createServer(app);
const io     = new Server(server, { cors: { origin: ORIGIN } });

app.use(helmet());
app.use(cors({ origin: ORIGIN }));
app.use(express.json());

const pub = createRedis();
const sub = createRedis();

await pub.connect();
await sub.connect();

// DOMF1302 — /health con ping Redis real, redis status y uptime
app.get('/health', async (_req, res) => {
  try {
    const ping = await pub.ping();
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

// UIUXF1101 — /kpis expone los KPIs calculados por Observability Service desde Redis
app.get('/kpis', async (_req, res) => {
  try {
    const today   = new Date().toISOString().split('T')[0];
    const raw     = await pub.hgetall(`stats:kpi:${today}`) ?? {};
    const started = parseInt(raw.games_started ?? 0);
    const ended   = parseInt(raw.games_ended   ?? 0);
    const disc    = parseInt(raw.disconnections ?? 0);
    const recon   = parseInt(raw.reconnections  ?? 0);
    const pico    = await pub.get('metrics:pico:salas') ?? 0;
    res.json({
      tasa_completacion: started ? ((ended   / started) * 100).toFixed(1) + '%' : 'N/A',
      tasa_reconexion:   disc    ? ((recon   / disc)    * 100).toFixed(1) + '%' : 'N/A',
      pico_salas:        pico,
      date:              today,
    });
  } catch (err) {
    res.status(503).json({ error: err.message });
  }
});

// DOMF102 — Verificación JWT (ya completado)
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

// DOMF002 — Rate limiting: ventana deslizante 1s, máx 20 eventos por IP
io.use(async (socket, next) => {
  const key   = `ratelimit:${socket.handshake.address}`;
  const count = await pub.incr(key);
  if (count === 1) await pub.expire(key, 1);
  if (count > 20) {
    log.warn('rate_limit_exceeded', socket.handshake.address);
    return next(new Error('rate_limit_exceeded'));
  }
  next();
});

// DOMF301 — Suscribirse a gw:broadcast, evt:TimerTick y evt:PhaseChanged
await sub.subscribe('gw:broadcast', 'evt:TimerTick', 'evt:PhaseChanged');

sub.on('message', (channel, raw) => {
  try {
    if (channel === 'gw:broadcast') {
      const { roomId, event, payload } = JSON.parse(raw);
      // Evento especial: Room Service pide que el socket se una a la sala Socket.io
      if (event === 'room:join-socket-room') {
        const socket = io.sockets.sockets.get(roomId);
        socket?.join(payload.codigo);
        log.info(`socket ${roomId} joined room ${payload.codigo}`);
        return;
      }
      io.to(roomId).emit(event, payload);
    } else if (channel === 'evt:TimerTick') {
      const { data } = JSON.parse(raw);
      io.to(data.codigo).emit('timer:tick', data);
    } else if (channel === 'evt:PhaseChanged') {
      // Proyecto.md §7.1: Gateway reenvía cambios de fase a todos los clientes de la sala
      const { data } = JSON.parse(raw);
      io.to(data.codigo).emit('phase:changed', data);
    }
  } catch (err) {
    log.error('broadcast parse fail —', err.message);
  }
});

io.on('connection', async (socket) => {
  log.info('cliente conectado:', socket.id);

  // DOMF401 — Reconexión: buscar si el jugador ya tiene sala activa en Redis
  const playerId = socket.user.sub;
  try {
    const keys = await pub.keys('sala:*');
    for (const key of keys) {
      // Saltar sub-claves (sala:{codigo}:chat, :energia:*, :salva:*)
      const parts = key.split(':');
      if (parts.length !== 2) continue;

      const raw = await pub.get(key);
      if (!raw) continue;
      const sala = JSON.parse(raw);
      const jugador = sala?.jugadores?.find((j) => j.id === playerId);
      if (jugador && sala.fase !== 'FIN') {
        socket.join(sala.codigo);
        jugador.socketId = socket.id;
        jugador.conectado = true;
        await pub.set(key, JSON.stringify(sala));
        await pub.publish('evt:PlayerReconnected', JSON.stringify({
          type:      'PlayerReconnected',
          source:    'gateway',
          timestamp: Date.now(),
          data:      { codigo: sala.codigo, playerId },
        }));
        socket.emit('game:state', sala);
        log.info(`reconexión: ${playerId} → sala ${sala.codigo}`);
        break;
      }
    }
  } catch (err) {
    log.error('error en reconexión:', err.message);
  }

  // DOMF002 — Enrutar eventos del cliente a los canales Redis correspondientes
  for (const [event, channel] of Object.entries(ROUTES)) {
    socket.on(event, (payload = {}) => {
      const message = {
        type:          event,
        source:        'gateway',
        timestamp:     Date.now(),
        version:       1,
        correlationId: crypto.randomUUID(),
        data: {
          socketId: socket.id,
          playerId: socket.user.sub,
          ...payload,
        },
      };
      pub.publish(channel, JSON.stringify(message));
      log.info(`${event} → ${channel}`);
    });
  }

  // DOMF002 — Notificar desconexión para que Room Service interprete
  socket.on('disconnect', () => {
    log.info('cliente desconectado:', socket.id);
    pub.publish('svc:room', JSON.stringify({
      type:      'PlayerDisconnected',
      source:    'gateway',
      timestamp: Date.now(),
      data:      { socketId: socket.id, playerId: socket.user.sub },
    }));
  });
});

server.listen(PORT, () => log.info(`:${PORT}`));
