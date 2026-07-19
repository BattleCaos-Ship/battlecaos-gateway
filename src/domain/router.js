export const ROUTES = {
  'room:create':          'cmd.room',
  'room:join':            'cmd.room',
  'room:cambiar-equipo':  'cmd.room',
  'room:comenzar':        'cmd.room',
  'room:salir':           'cmd.room',
  'room:volver':          'cmd.room',
  'disparo:realizar':     'cmd.game',
  'salva:disparo':        'cmd.game',
  'poder:usar':           'cmd.game',
  'colocacion:set':       'cmd.game',
  'contramedida:activar': 'cmd.game',
  'chat:mensaje':         'cmd.chat',
  'voice:join':           'cmd.voice',
  'voice:leave':          'cmd.voice',
  'voice:mute':           'cmd.voice',
};

export function buildMessage(event, socketId, playerId, payload = {}) {
  return {
    type:          event,
    source:        'gateway',
    timestamp:     Date.now(),
    version:       1,
    correlationId: crypto.randomUUID(),
    data: {
      // El payload va PRIMERO y socketId/playerId DESPUÉS: así los campos de identidad
      // verificados por el servidor (playerId = socket.user.sub del JWT) SIEMPRE ganan
      // sobre cualquier `playerId`/`socketId` que un cliente malicioso intente inyectar
      // en el payload. Sin esto, un jugador podía actuar como otro suplantando su id.
      ...payload,
      socketId,
      playerId,
    },
  };
}
