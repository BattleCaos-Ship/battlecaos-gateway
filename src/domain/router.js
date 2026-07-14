export const ROUTES = {
  'room:create':          'cmd.room',
  'room:join':            'cmd.room',
  'room:cambiar-equipo':  'cmd.room',
  'room:comenzar':        'cmd.room',
  'room:salir':           'cmd.room',
  'disparo:realizar':     'cmd.game',
  'salva:disparo':        'cmd.game',
  'poder:usar':           'cmd.game',
  'colocacion:set':       'cmd.game',
  'contramedida:activar': 'cmd.game',
  'chat:mensaje':         'cmd.chat',
};

export function buildMessage(event, socketId, playerId, payload = {}) {
  return {
    type:          event,
    source:        'gateway',
    timestamp:     Date.now(),
    version:       1,
    correlationId: crypto.randomUUID(),
    data: {
      socketId,
      playerId,
      ...payload,
    },
  };
}
