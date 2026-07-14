import client from 'prom-client';

export const register = new client.Registry();
client.collectDefaultMetrics({ register, prefix: 'gateway_' });

export const httpDuration = new client.Histogram({
  name: 'http_server_requests_seconds',
  help: 'Duración de solicitudes HTTP',
  labelNames: ['method', 'route', 'status'],
  buckets: [0.05, 0.1, 0.3, 0.5, 1, 2, 5],
  registers: [register],
});

export const socketConnectionsTotal = new client.Counter({
  name: 'gateway_socket_connections_total',
  help: 'Conexiones Socket.io aceptadas',
  registers: [register],
});

export const eventsRoutedTotal = new client.Counter({
  name: 'gateway_events_routed_total',
  help: 'Eventos de cliente enrutados a Kafka, por tipo',
  labelNames: ['event'],
  registers: [register],
});

export const rateLimitExceededTotal = new client.Counter({
  name: 'gateway_rate_limit_exceeded_total',
  help: 'Conexiones/eventos rechazados por rate limiting',
  registers: [register],
});

export const reconnectionsTotal = new client.Counter({
  name: 'gateway_reconnections_total',
  help: 'Jugadores reconectados a una sala activa',
  registers: [register],
});

export const kafkaConsumerCrashTotal = new client.Counter({
  name: 'gateway_kafka_consumer_crash_total',
  help: 'Veces que el consumer de Kafka del gateway crasheó',
  registers: [register],
});

export const activeSocketsGauge = new client.Gauge({
  name: 'gateway_active_sockets',
  help: 'Sockets conectados actualmente',
  registers: [register],
});
