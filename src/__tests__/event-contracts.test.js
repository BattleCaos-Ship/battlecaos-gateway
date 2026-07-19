import { describe, it, expect } from 'vitest';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { ROUTES } from '../domain/router.js';

// Contrato formal (docs/event-contracts.md, en la raíz del monorepo). Este test garantiza que el
// documento y el código NO se desincronicen: cada ruta del gateway debe estar documentada y cada
// comando documentado debe existir en ROUTES.
const aqui = dirname(fileURLToPath(import.meta.url));
const docPath = resolve(aqui, '../../../docs/event-contracts.md');
const doc = readFileSync(docPath, 'utf8');

// Extrae los comandos documentados en la tabla "Comandos de cliente": celdas `evento` con backticks
// cuyo topic es cmd.*  → filas tipo: | `room:create` | `cmd.room` | room |
function comandosDocumentados(md) {
  const set = new Set();
  for (const linea of md.split('\n')) {
    const m = linea.match(/^\|\s*`([^`]+)`\s*\|\s*`(cmd\.[^`]+)`\s*\|/);
    if (m) set.add(m[1]);
  }
  return set;
}

describe('contrato de eventos (docs/event-contracts.md ↔ ROUTES)', () => {
  const documentados = comandosDocumentados(doc);

  it('cada ruta del gateway está documentada', () => {
    const faltan = Object.keys(ROUTES).filter((ev) => !documentados.has(ev));
    expect(faltan, `Rutas SIN documentar en event-contracts.md: ${faltan.join(', ')}`).toEqual([]);
  });

  it('cada comando documentado existe en ROUTES (no hay documentación obsoleta)', () => {
    const sobran = [...documentados].filter((ev) => !(ev in ROUTES));
    expect(sobran, `Comandos documentados que ya no existen en ROUTES: ${sobran.join(', ')}`).toEqual([]);
  });

  it('el topic documentado coincide con el de ROUTES', () => {
    // Reconstruye evento→topic desde el doc y compara con ROUTES.
    const docTopic = {};
    for (const linea of doc.split('\n')) {
      const m = linea.match(/^\|\s*`([^`]+)`\s*\|\s*`(cmd\.[^`]+)`\s*\|/);
      if (m) docTopic[m[1]] = m[2];
    }
    for (const [ev, topic] of Object.entries(ROUTES)) {
      expect(docTopic[ev], `topic de ${ev}`).toBe(topic);
    }
  });
});
