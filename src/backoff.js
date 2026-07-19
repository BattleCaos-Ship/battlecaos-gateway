// Backoff exponencial con JITTER para reconexiones (disponibilidad §10, "retry con backoff").
//
// El reconnect fijo de 5s tenía dos problemas: (1) si Kafka tarda en volver, 5s fijos reintentan
// en vano y saturan; (2) cuando Kafka SÍ vuelve, TODOS los consumers reconectan al mismo tiempo
// (thundering herd) y le pegan un pico. El backoff exponencial espaciando reintentos + el jitter
// (ruido aleatorio) desincroniza a los clientes → la recuperación es más suave y estable.
export function crearBackoff({ baseMs = 1000, maxMs = 30000 } = {}) {
  let intento = 0;
  return {
    // Delay del PRÓXIMO reintento: crece exponencial (base·2^intento) hasta maxMs, con jitter
    // en [mitad, total] para que dos procesos no reintenten en el mismo instante.
    siguiente() {
      const tope = Math.min(maxMs, baseMs * 2 ** intento);
      intento++;
      return Math.round(tope / 2 + Math.random() * (tope / 2));
    },
    // Tras una reconexión exitosa (GROUP_JOIN) se resetea → el próximo fallo vuelve a empezar corto.
    reset() { intento = 0; },
    get intentos() { return intento; },
  };
}
