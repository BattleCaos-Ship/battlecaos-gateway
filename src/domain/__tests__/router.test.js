import { describe, it, expect } from 'vitest';
import { ROUTES, buildMessage } from '../router.js';

// ── ROUTES ────────────────────────────────────────────────────────────────────

describe('ROUTES', () => {
  it('mapea room:create a svc:room', () => {
    expect(ROUTES['room:create']).toBe('svc:room');
  });

  it('mapea room:join a svc:room', () => {
    expect(ROUTES['room:join']).toBe('svc:room');
  });

  it('mapea disparo:realizar a svc:game', () => {
    expect(ROUTES['disparo:realizar']).toBe('svc:game');
  });

  it('mapea salva:disparo a svc:game', () => {
    expect(ROUTES['salva:disparo']).toBe('svc:game');
  });

  it('mapea poder:usar a svc:game', () => {
    expect(ROUTES['poder:usar']).toBe('svc:game');
  });

  it('mapea colocacion:set a svc:game', () => {
    expect(ROUTES['colocacion:set']).toBe('svc:game');
  });

  it('mapea contramedida:activar a svc:game', () => {
    expect(ROUTES['contramedida:activar']).toBe('svc:game');
  });

  it('mapea chat:mensaje a svc:chat', () => {
    expect(ROUTES['chat:mensaje']).toBe('svc:chat');
  });

  it('tiene exactamente 8 rutas definidas', () => {
    expect(Object.keys(ROUTES)).toHaveLength(8);
  });

  it('todos los valores son canales Redis válidos (svc:*)', () => {
    const channels = Object.values(ROUTES);
    for (const ch of channels) {
      expect(ch).toMatch(/^svc:/);
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
});
