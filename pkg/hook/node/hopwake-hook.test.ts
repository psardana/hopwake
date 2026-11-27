import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createServer, type IncomingMessage, type Server } from 'node:http';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { Hook, formatAlias, formatTime, journeyId, parseDuration, timeJourneyId, type HookConfig } from './hopwake-hook.ts';

const vectors = JSON.parse(readFileSync(new URL('../testdata/vectors.json', import.meta.url), 'utf8'));

test('journey IDs of readings with a sequence number', () => {
  for (const v of vectors.seq) {
    assert.equal(journeyId(v.tenant, v.device, BigInt(v.seq), v.boot), v.id, JSON.stringify(v));
  }
});

test('times and journey IDs of readings known by time', () => {
  for (const v of vectors.time) {
    const ns = BigInt(v.unix_nanos);
    const p = BigInt(v.precision_ns);
    assert.equal(formatTime(ns, p), v.formatted, JSON.stringify(v));
    assert.equal(timeJourneyId(v.tenant, v.device, v.type, ns, p), v.id, JSON.stringify(v));
    if (v.resolution === 's' || v.resolution === 'ms') {
      const at = new Date(Number(ns / 1_000_000n));
      assert.equal(formatTime(at, p), v.formatted, JSON.stringify(v));
      assert.equal(timeJourneyId(v.tenant, v.device, v.type, at, p), v.id, JSON.stringify(v));
    }
  }
});

test('aliases', () => {
  for (const v of vectors.alias) assert.equal(formatAlias(v.kind, v.value), v.formatted);
});

test('durations read as Go reads them', () => {
  const ok: [string, bigint][] = [
    ['0', 0n], ['1s', 1_000_000_000n], ['500ms', 500_000_000n], ['1m', 60_000_000_000n],
    ['1h30m', 5_400_000_000_000n], ['1.5s', 1_500_000_000n], ['.5s', 500_000_000n], ['10µs', 10_000n],
    ['10us', 10_000n], ['7ns', 7n], ['-1s', -1_000_000_000n], ['+2s', 2_000_000_000n],
  ];
  for (const [s, want] of ok) assert.equal(parseDuration(s), want, s);
  for (const s of ['', '1', 's', '1x', '.s', '1s2', '-', '9999999999h']) assert.throws(() => parseDuration(s), undefined, s);
});

// A stand-in for Hopwake: collects spans and serves an alias list.
type Got = { service: string; span: any; attrs: Record<string, string> };
async function hopwake(aliases: unknown = []) {
  const got: Got[] = [];
  const server: Server = createServer(async (req: IncomingMessage, res) => {
    if (req.method === 'GET' && req.url === '/api/aliases') {
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify(aliases));
      return;
    }
    let body = '';
    for await (const chunk of req) body += chunk;
    for (const rs of JSON.parse(body).resourceSpans) {
      const service = rs.resource.attributes.find((a: any) => a.key === 'service.name').value.stringValue;
      for (const ss of rs.scopeSpans) {
        for (const span of ss.spans) {
          const attrs: Record<string, string> = {};
          for (const a of span.attributes) attrs[a.key] = a.value.stringValue ?? a.value.intValue;
          got.push({ service, span, attrs });
        }
      }
    }
    res.end('{}');
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const { port } = server.address() as { port: number };
  const url = `http://127.0.0.1:${port}`;
  return { got, url, close: () => new Promise((r) => server.close(r)) };
}

function hookFor(url: string, cfg: Partial<HookConfig> = {}): Hook {
  return new Hook({ tenant: 'conformance', hop: 'hook:test', endpoint: url, aliases: url, ...cfg });
}

async function until(ok: () => boolean, ms = 3000) {
  const end = Date.now() + ms;
  while (!ok()) {
    if (Date.now() > end) throw new Error('timed out');
    await new Promise((r) => setTimeout(r, 10));
  }
}

const at = new Date('2026-11-18T10:00:00.750Z');
const typ = 'ems.measurements.Measurement';

test('a mark on the wire', async () => {
  const hw = await hopwake();
  const h = hookFor(hw.url, { service: 'svc' });
  h.mark('store', 'dev-00042', 1683);
  await h.close();
  await hw.close();
  assert.equal(hw.got.length, 1);
  const { service, span, attrs } = hw.got[0];
  const id = journeyId('conformance', 'dev-00042', 1683);
  assert.equal(service, 'svc');
  assert.equal(span.traceId, id);
  assert.match(span.spanId, /^[0-9a-f]{16}$/);
  assert.notEqual(span.spanId, '0000000000000000');
  assert.equal(span.name, 'hop hook:test');
  assert.equal(span.kind, 5);
  assert.equal(span.startTimeUnixNano, span.endTimeUnixNano);
  assert.ok(Math.abs(Number(BigInt(span.startTimeUnixNano) / 1_000_000n) - Date.now()) < 60_000);
  assert.deepEqual(attrs, {
    'hopwake.schema': 'hop/v1',
    'hopwake.tenant': 'conformance',
    'hopwake.device.id': 'dev-00042',
    'hopwake.seq': '1683',
    'hopwake.journey.id': id,
    'hopwake.hop': 'hook:test',
    'hopwake.op': 'store',
    'hopwake.correlation': 'derived',
    'hopwake.payload.bytes': '0',
    'messaging.system': 'hook',
  });
});

test('a reading is marked served once, stored always', async () => {
  const hw = await hopwake();
  const h = hookFor(hw.url);
  h.mark('serve', 'dev-00042', 1683);
  h.mark('serve', 'dev-00042', 1683);
  h.markBoot('serve', 'dev-00042', '7', 1683);
  h.markTime('serve', 'EMS0000042', typ, at);
  h.markTime('serve', 'EMS0000042', typ, at);
  h.mark('store', 'dev-00042', 1683);
  h.mark('store', 'dev-00042', 1683);
  await h.close();
  await hw.close();
  assert.deepEqual(hw.got.map((g) => g.attrs['hopwake.op']), ['serve', 'serve', 'serve', 'store', 'store']);
});

test('time, teach and alias marks', async () => {
  const hw = await hopwake([{ kind: 'systemId', value: '100042', device: 'EMS0000042' }]);
  const h = hookFor(hw.url, { timePrecision: '1s' });
  h.markTime('store', 'meter-7', '', at);
  assert.equal(h.markTimeAlias('serve', 'systemId', '100042', typ, at), false); // the list has not arrived yet
  await until(() => h.markTimeAlias('serve', 'systemId', '100042', typ, at));
  h.markTimeTeach('store', 'EMS0000042', 'systemId', '100042', typ, at);
  await h.close();
  await hw.close();
  const [plain, alias, teach] = hw.got.map((g) => g.attrs);
  assert.equal(plain['hopwake.journey.id'], timeJourneyId('conformance', 'meter-7', '', 1_794_996_000_000_000_000n));
  assert.equal(plain['hopwake.seq'], '0');
  assert.equal(plain['hopwake.message.type'], undefined);
  assert.equal(plain['hopwake.device.alias'], undefined);
  const id = timeJourneyId('conformance', 'EMS0000042', typ, at, 1_000_000_000n);
  for (const a of [alias, teach]) {
    assert.equal(a['hopwake.journey.id'], id);
    assert.equal(a['hopwake.device.id'], 'EMS0000042');
    assert.equal(a['hopwake.message.type'], typ);
    assert.equal(a['hopwake.device.alias'], 'systemId:100042');
    assert.equal(a['hopwake.seq'], '0');
  }
  assert.equal(alias['hopwake.op'], 'serve');
  assert.equal(teach['hopwake.op'], 'store');
});

test('an unknown alias marks nothing', async () => {
  const hw = await hopwake([{ kind: 'systemId', value: '100042', device: 'EMS0000042' }]);
  const h = hookFor(hw.url);
  await until(() => h.markTimeAlias('store', 'systemId', '100042', typ, at));
  assert.equal(h.markTimeAlias('serve', 'systemId', '999999', typ, at), false);
  assert.equal(h.markTimeAlias('serve', 'vin', '100042', typ, at), false);
  await h.close();
  const noList = hookFor(hw.url, { aliases: undefined });
  assert.equal(noList.markTimeAlias('serve', 'systemId', '100042', typ, at), false);
  await noList.close();
  await hw.close();
  assert.equal(hw.got.length, 1);
});

test('settings come from the environment unless given in code', async () => {
  const hw = await hopwake([{ kind: 'systemId', value: '1', device: 'D1' }]);
  const saved = { ...process.env };
  Object.assign(process.env, { HOPWAKE_TENANT: 'envtenant', HOPWAKE_OTLP_ENDPOINT: hw.url, HOPWAKE_API: hw.url, HOPWAKE_TIME_PRECISION: '1m' });
  try {
    const h = new Hook({ hop: 'hook:test' });
    await until(() => h.markTimeAlias('store', 'systemId', '1', typ, at));
    const given = new Hook({ tenant: 'mine', hop: 'hook:test', timePrecision: 500 });
    given.markTime('store', 'D1', typ, at);
    await h.close();
    await given.close();
  } finally {
    process.env = saved;
  }
  await hw.close();
  const [env, code] = hw.got.map((g) => g.attrs);
  assert.equal(env['hopwake.tenant'], 'envtenant');
  assert.equal(env['hopwake.journey.id'], timeJourneyId('envtenant', 'D1', typ, at, 60_000_000_000n));
  assert.equal(code['hopwake.tenant'], 'mine');
  assert.equal(code['hopwake.journey.id'], timeJourneyId('mine', 'D1', typ, at, 500_000_000n));
  assert.equal(hw.got[0].service, 'hook:test');
});

test('a hook needs a tenant and a hop', () => {
  const saved = process.env.HOPWAKE_TENANT;
  delete process.env.HOPWAKE_TENANT;
  try {
    assert.throws(() => new Hook({ hop: 'hook:test' }));
    assert.throws(() => new Hook({ tenant: 't', hop: '' }));
  } finally {
    if (saved !== undefined) process.env.HOPWAKE_TENANT = saved;
  }
});

test('a full queue drops new marks', async () => {
  const hw = await hopwake();
  const h = hookFor(hw.url, { maxQueue: 3 });
  for (let seq = 1; seq <= 5; seq++) h.mark('store', 'dev-00042', seq);
  await h.close();
  await hw.close();
  assert.deepEqual(hw.got.map((g) => g.attrs['hopwake.seq']), ['1', '2', '3']);
});

test('bad input never throws into the service', async () => {
  const h = hookFor('http://127.0.0.1:9', { aliases: undefined });
  h.mark('store', 'd', -1);
  h.mark('store', 'd', 1.5);
  h.markTime('store', 'd', typ, new Date('not a time'));
  await h.close();
});

test('an unreachable endpoint costs the service nothing', async () => {
  // Port 9 on loopback refuses connections.
  const h = hookFor('http://127.0.0.1:9');
  const start = Date.now();
  for (let seq = 0; seq < 10_000; seq++) h.mark('store', 'dev-00042', seq);
  assert.ok(Date.now() - start < 2000, 'marking took too long');
  const closing = Date.now();
  await h.close();
  assert.ok(Date.now() - closing < 1000, `close took ${Date.now() - closing} ms`);
});

test('a send that hangs is given up on after 3 seconds', async () => {
  const sockets = new Set<any>();
  const silent = createServer(() => {}); // never answers
  silent.on('connection', (s) => sockets.add(s));
  silent.listen(0, '127.0.0.1');
  await once(silent, 'listening');
  const { port } = silent.address() as { port: number };
  const h = hookFor(`http://127.0.0.1:${port}`, { aliases: undefined });
  h.mark('store', 'dev-00042', 1);
  const start = Date.now();
  await h.close();
  const took = Date.now() - start;
  for (const s of sockets) s.destroy();
  silent.close();
  assert.ok(took >= 2500 && took < 4500, `close took ${took} ms`);
});

test('the conformance program fails when an unknown alias is marked', async () => {
  // This Hopwake knows 999999 too, so step 8 must fail.
  const hw = await hopwake([
    { kind: 'systemId', value: '100042', device: 'EMS0000042' },
    { kind: 'systemId', value: '999999', device: 'EMS0000999' },
  ]);
  const child = spawn(process.execPath, [...process.execArgv.filter((a) => a.includes('strip-types')), new URL('./conformance.ts', import.meta.url).pathname], {
    env: { ...process.env, HOPWAKE_OTLP_ENDPOINT: hw.url, HOPWAKE_API: hw.url, HOPWAKE_TIME_PRECISION: '1s', HOPWAKE_TENANT: 'conformance', HOPWAKE_HOP: 'hook:conformance.check' },
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  let stderr = '';
  child.stderr.on('data', (b) => (stderr += b));
  const [code] = await once(child, 'exit');
  await hw.close();
  assert.notEqual(code, 0);
  assert.match(stderr, /an unknown alias was marked/);
});
