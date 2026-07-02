import pino from 'pino';

const usePretty = process.env.NODE_ENV !== 'production' && process.env.NODE_ENV !== 'test';

const _pino = pino({
  name: 'gateway',
  level: process.env.LOG_LEVEL ?? 'info',
  ...(usePretty && {
    transport: {
      target: 'pino-pretty',
      options: { colorize: true, translateTime: 'SYS:HH:MM:ss', ignore: 'pid,hostname' },
    },
  }),
});

const wrap = (level) => (...args) => {
  if (typeof args[0] === 'object' && args[0] !== null) {
    _pino[level](args[0], args[1] ?? '');
  } else {
    _pino[level](args.join(' '));
  }
};

export const log = {
  debug: wrap('debug'),
  info:  wrap('info'),
  warn:  wrap('warn'),
  error: wrap('error'),
};
