// Reference pipeline API: serves stored telemetry to the portal and to anyone else.
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import pg from 'pg';
import { Hook } from './hopwake-hook.ts';

const port = Number(process.env.API_PORT ?? 8080);
const pool = new pg.Pool({ connectionString: process.env.API_DB ?? 'postgres://hopwake:hopwake@localhost:5432/reference' });
const portalDir = path.join(import.meta.dirname, 'portal');

// Hopwake cannot see an HTTP response from outside, so the API says itself
// which readings it served, when HOPWAKE_OTLP_ENDPOINT is set.
const served = process.env.HOPWAKE_OTLP_ENDPOINT
  ? new Hook({ tenant: process.env.HOPWAKE_TENANT ?? 'reference', hop: 'hook:api.served', service: 'api' })
  : null;

type Handler = (params: Record<string, string>, query: URLSearchParams) => Promise<unknown>;

const routes: Array<[RegExp, Handler]> = [
  // Latest closed minute per site.
  [/^\/api\/sites$/, async () => (await pool.query(`
    SELECT DISTINCT ON (site) site, window_start, window_end, readings, devices,
           round((median_temperature_k - 273.15)::numeric, 2)::float AS median_temperature_c,
           median_pressure_pa
    FROM site_minute ORDER BY site, window_start DESC`)).rows],

  // Recent minutes of one site, oldest first.
  [/^\/api\/sites\/(?<site>[\w.-]+)\/minutes$/, async (p, q) => (await pool.query(`
    SELECT * FROM (
      SELECT window_start, readings, devices,
             round((median_temperature_k - 273.15)::numeric, 2)::float AS median_temperature_c, median_pressure_pa
      FROM site_minute WHERE site = $1 ORDER BY window_start DESC LIMIT $2) m
    ORDER BY window_start`, [p.site, limit(q, 60, 1440)])).rows],

  // Devices of a site with when each was last seen.
  [/^\/api\/sites\/(?<site>[\w.-]+)\/devices$/, async (p) => (await pool.query(`
    SELECT device, max(observed_at) AS last_seen, max(seq)::int AS last_seq
    FROM readings WHERE site = $1 GROUP BY device ORDER BY device`, [p.site])).rows],

  // Latest readings of one device, newest first.
  [/^\/api\/devices\/(?<device>[\w.-]+)\/readings$/, async (p, q) => {
    const rows = (await pool.query(`
      SELECT seq::int AS seq, site, observed_at, stored_at,
             round((temperature_k - 273.15)::numeric, 2)::float AS temperature_c,
             pressure_pa, vibration_mm_s
      FROM readings WHERE device = $1 ORDER BY observed_at DESC LIMIT $2`, [p.device, limit(q, 50, 1000)])).rows;
    for (const r of rows) served?.mark('serve', p.device, r.seq);
    return rows;
  }],
];

function limit(q: URLSearchParams, fallback: number, max: number): number {
  const n = Number(q.get('limit') ?? fallback);
  return Number.isInteger(n) && n > 0 ? Math.min(n, max) : fallback;
}

function send(res: http.ServerResponse, status: number, body: unknown) {
  res.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' });
  res.end(JSON.stringify(body));
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url ?? '/', 'http://localhost');
  try {
    if (req.method !== 'GET') return send(res, 405, { error: 'method not allowed' });
    if (url.pathname === '/' || url.pathname === '/index.html') {
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
      return res.end(await readFile(path.join(portalDir, 'index.html')));
    }
    if (url.pathname === '/healthz') {
      await pool.query('SELECT 1');
      return send(res, 200, { ok: true });
    }
    for (const [pattern, handler] of routes) {
      const m = pattern.exec(url.pathname);
      if (m) return send(res, 200, await handler(m.groups ?? {}, url.searchParams));
    }
    send(res, 404, { error: 'not found' });
  } catch (err) {
    console.error(req.url, err);
    send(res, 500, { error: 'internal error' });
  }
});

server.listen(port, () => console.log(`api on :${port}`));
process.on('SIGTERM', () => server.close(() => pool.end().then(() => process.exit(0))));
