// Trazado distribuido con OpenTelemetry (alternativa LIBRE a un APM de pago).
// OPT-IN y A PRUEBA DE FALLOS: solo se activa si OTEL_EXPORTER_OTLP_ENDPOINT está definido
// Y las dependencias de OTel están instaladas. Sin eso NO hace nada → cero impacto en el
// despliegue actual (donde Jaeger no corre y las deps de OTel pueden no estar).
//
// Debe importarse ANTES que cualquier otro módulo (parchea http/kafkajs/ioredis en carga).
// Para instrumentación completa en ESM, arrancar con:
//   node --import ./src/tracing.js src/index.js
import process from 'node:process';

const endpoint = process.env.OTEL_EXPORTER_OTLP_ENDPOINT;

if (endpoint) {
  try {
    const { NodeSDK }                     = await import('@opentelemetry/sdk-node');
    const { OTLPTraceExporter }           = await import('@opentelemetry/exporter-trace-otlp-http');
    const { resourceFromAttributes }      = await import('@opentelemetry/resources');
    const { ATTR_SERVICE_NAME }           = await import('@opentelemetry/semantic-conventions');
    const { getNodeAutoInstrumentations } = await import('@opentelemetry/auto-instrumentations-node');

    const sdk = new NodeSDK({
      resource: resourceFromAttributes({
        [ATTR_SERVICE_NAME]: process.env.SERVICE_NAME ?? 'battlecaos-servicio',
      }),
      traceExporter: new OTLPTraceExporter({ url: `${endpoint}/v1/traces` }),
      instrumentations: [getNodeAutoInstrumentations({
        '@opentelemetry/instrumentation-fs': { enabled: false },
      })],
    });
    sdk.start();
    // eslint-disable-next-line no-console
    console.log(`[otel] trazado activo → ${endpoint} (servicio ${process.env.SERVICE_NAME ?? '?'})`);
    process.on('SIGTERM', () => { sdk.shutdown().finally(() => process.exit(0)); });
  } catch (err) {
    // Deps de OTel no instaladas o fallo al iniciar: seguir SIN trazas (no tumbar el servicio).
    // eslint-disable-next-line no-console
    console.warn(`[otel] trazado NO activado (${err.message}). Instala las deps de OpenTelemetry para habilitarlo.`);
  }
}
