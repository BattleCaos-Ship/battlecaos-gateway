import { describe, it, expect } from 'vitest';
import { ROUTES, buildMessage } from '../router.js';

// ── ROUTES ────────────────────────────────────────────────────────────────────

describe('ROUTES', () => {
  it('mapea room:create a cmd.room', () => {
    expect(ROUTES['room:create']).toBe('cmd.room');
  });

  it('mapea room:join a cmd.room', () => {
    expect(ROUTES['room:join']).toBe('cmd.room');
  });

  it('mapea los eventos de lobby (equipo/comenzar/salir) a cmd.room', () => {
    expect(ROUTES['room:cambiar-equipo']).toBe('cmd.room');
    expect(ROUTES['room:comenzar']).toBe('cmd.room');
    expect(ROUTES['room:salir']).toBe('cmd.room');
  });

  it('mapea disparo:realizar a cmd.game', () => {
    expect(ROUTES['disparo:realizar']).toBe('cmd.game');
  });

  it('mapea salva:disparo a cmd.game', () => {
    expect(ROUTES['salva:disparo']).toBe('cmd.game');
  });

  it('mapea poder:usar a cmd.game', () => {
    expect(ROUTES['poder:usar']).toBe('cmd.game');
  });

  it('mapea colocacion:set a cmd.game', () => {
    expect(ROUTES['colocacion:set']).toBe('cmd.game');
  });

  it('mapea contramedida:activar a cmd.game', () => {
    expect(ROUTES['contramedida:activar']).toBe('cmd.game');
  });

  it('mapea chat:mensaje a cmd.chat', () => {
    expect(ROUTES['chat:mensaje']).toBe('cmd.chat');
  });

  it('tiene exactamente 15 rutas definidas', () => {
    expect(Object.keys(ROUTES)).toHaveLength(15);
  });

  it('incluye la ruta de volver a la sala → cmd.room', () => {
    expect(ROUTES['room:volver']).toBe('cmd.room');
  });

  it('incluye las rutas de voz → cmd.voice', () => {
    expect(ROUTES['voice:join']).toBe('cmd.voice');
    expect(ROUTES['voice:leave']).toBe('cmd.voice');
    expect(ROUTES['voice:mute']).toBe('cmd.voice');
  });

  it('todos los valores son topics Kafka válidos (cmd.*)', () => {
    const topics = Object.values(ROUTES);
    for (const t of topics) {
      expect(t).toMatch(/^cmd\./);
    }
  });
});

// ── buildMessage ──────────────────────────────────────────────────────────────

describe('buildMessage', () => {
  it('incluye el type correcto', () => {
    const msg = buildMessage('room:create', 's-1', 'uid-1', {});
    expect(msg.type).toBe('room:create');
  });

  it('source es siempre gateway', () => {
    const msg = buildMessage('room:join', 's-1', 'uid-1', {});
    expect(msg.source).toBe('gateway');
  });

  it('version es siempre 1', () => {
    const msg = buildMessage('room:join', 's-1', 'uid-1', {});
    expect(msg.version).toBe(1);
  });

  it('timestamp es un número cercano a Date.now()', () => {
    const before = Date.now();
    const msg = buildMessage('room:join', 's-1', 'uid-1', {});
    const after = Date.now();
    expect(msg.timestamp).toBeGreaterThanOrEqual(before);
    expect(msg.timestamp).toBeLessThanOrEqual(after);
  });

  it('correlationId es un UUID válido (formato estándar)', () => {
    const msg = buildMessage('room:create', 's-1', 'uid-1', {});
    expect(msg.correlationId).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
    );
  });

  it('dos llamadas generan correlationIds distintos', () => {
    const m1 = buildMessage('room:create', 's-1', 'uid-1', {});
    const m2 = buildMessage('room:create', 's-1', 'uid-1', {});
    expect(m1.correlationId).not.toBe(m2.correlationId);
  });

  it('data incluye socketId', () => {
    const msg = buildMessage('room:create', 'socket-abc', 'uid-1', {});
    expect(msg.data.socketId).toBe('socket-abc');
  });

  it('data incluye playerId', () => {
    const msg = buildMessage('room:create', 's-1', 'google-uid-xyz', {});
    expect(msg.data.playerId).toBe('google-uid-xyz');
  });

  it('data incluye los campos extra del payload', () => {
    const msg = buildMessage('room:create', 's-1', 'uid-1', { modo: '1v1', codigo: '123456' });
    expect(msg.data.modo).toBe('1v1');
    expect(msg.data.codigo).toBe('123456');
  });

  it('payload vacío no añade campos extra en data', () => {
    const msg = buildMessage('room:join', 's-1', 'uid-1');
    expect(Object.keys(msg.data)).toEqual(['socketId', 'playerId']);
  });

  // SEGURIDAD: el cliente no puede suplantar a otro jugador inyectando playerId en el payload.
  it('un playerId inyectado en el payload NO sobreescribe el verificado por JWT', () => {
    const msg = buildMessage('disparo:realizar', 's-1', 'uid-real', {
      playerId: 'uid-de-la-victima', // intento de suplantación
      codigo: '123456',
    });
    expect(msg.data.playerId).toBe('uid-real'); // gana el del servidor, no el del cliente
    expect(msg.data.codigo).toBe('123456');     // el resto del payload sí pasa
  });

  it('un socketId inyectado en el payload tampoco sobreescribe el real', () => {
    const msg = buildMessage('room:salir', 'socket-real', 'uid-1', { socketId: 'socket-falso' });
    expect(msg.data.socketId).toBe('socket-real');
  });
});
