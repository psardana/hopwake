// Hopwake hook for Node services: lets a service say that it stored or
// served readings, for hops no tap can watch from outside.
//
//   const served = new Hook({ tenant: 'reference', hop: 'hook:api.served' });
//   served.mark('serve', 'dev-00042', 1683);
//   served.markTime('store', 'EMS0000042', 'ems.measurements.Measurement', at);
//
// Marks are hop events sent as OTLP/JSON spans to Hopwake's collector, keyed
// by the same journey ID a tap derives. They are batched, sent at least once
// a second, and dropped quietly when Hopwake cannot be reached: the service
// is never slowed or stopped by its hook. A reading is marked served only the
// first time, however often it is asked for.
//
// The contract every hook follows is in docs/hooks.md. This file has no
// dependencies, so it can be copied into a service as it is.
import { createHash, randomBytes } from 'node:crypto';

export type Op = 'store' | 'serve';

export interface HookConfig {
  tenant?: string; // default $HOPWAKE_TENANT; required
  hop: string; // matches a hook source in the rules file: "hook:" + match
  endpoint?: string; // default $HOPWAKE_OTLP_ENDPOINT, else http://localhost:4318
  service?: string; // service.name on the spans; default the hop
  // The rules file's time_precision, in milliseconds or as a duration such
  // as '1s' or '500ms'; default $HOPWAKE_TIME_PRECISION, else the time as given.
  timePrecision?: number | string;
  // The journey service's base URL, for markTimeAlias; default $HOPWAKE_API.
  aliases?: string;
  maxQueue?: number; // marks waiting to be sent; default 50,000
}

const NS = 1_000_000_000n;
const SERVED_LIMIT = 100_000;
const BATCH = 1000;
const SEND_TIMEOUT_MS = 3000;
const CLOSE_WAIT_MS = 5000;
const ALIAS_EVERY_MS = 5000;

function field(s: string): Buffer {
  const b = Buffer.from(s, 'utf8');
  const n = Buffer.alloc(4);
  n.writeUInt32BE(b.length);
  return Buffer.concat([n, b]);
}

function digest(h: ReturnType<typeof createHash>): string {
  const id = h.digest().subarray(0, 16);
  if (id.every((x) => x === 0)) id[15] = 1;
  return id.toString('hex');
}

/** The journey ID of one reading, as Hopwake derives it (32 hex characters). */
export function journeyId(tenant: string, device: string, seq: number | bigint, boot = ''): string {
  const h = createHash('sha256');
  h.update(field('hopwake/journey/v1'));
  h.update(field(tenant));
  h.update(field(device));
  if (boot !== '') h.update(field(`boot:${boot}`));
  const s = Buffer.alloc(8);
  s.writeBigUInt64BE(BigInt(seq));
  h.update(s);
  return digest(h);
}

/**
 * The journey ID of a reading with no sequence number: device, message type
 * and its own time, rounded down to precisionNs.
 */
export function timeJourneyId(tenant: string, device: string, messageType: string, at: Date | bigint, precisionNs: bigint | number = 0n): string {
  const h = createHash('sha256');
  h.update(field('hopwake/journey/time/v1'));
  h.update(field(tenant));
  h.update(field(device));
  h.update(field(`type:${messageType}`));
  h.update(field(`time:${formatTime(at, precisionNs)}`));
  return digest(h);
}

function toNanos(at: Date | bigint): bigint {
  return typeof at === 'bigint' ? at : BigInt(at.getTime()) * 1_000_000n;
}

// Go's zero time (1 January of year 1) to the Unix epoch. Rounding down from
// there is the same as from the epoch for every precision the contract
// allows, and agrees with the taps for any other.
const YEAR1_TO_UNIX_NS = 62_135_596_800n * NS;

/**
 * A reading's time as it enters a journey ID: rounded down to precisionNs
 * (0 keeps it), in UTC, with up to nine fraction digits and no trailing zeros.
 * A bigint is nanoseconds since the Unix epoch.
 */
export function formatTime(at: Date | bigint, precisionNs: bigint | number = 0n): string {
  let ns = toNanos(at);
  const p = BigInt(precisionNs);
  if (p > 0n) ns -= (((ns + YEAR1_TO_UNIX_NS) % p) + p) % p;
  let sec = ns / NS;
  if (ns % NS < 0n) sec -= 1n;
  const frac = (ns - sec * NS).toString().padStart(9, '0').replace(/0+$/, '');
  const whole = new Date(Number(sec) * 1000).toISOString().slice(0, 19);
  return frac === '' ? `${whole}Z` : `${whole}.${frac}Z`;
}

/** An alias as it travels on hop events: "kind:value". */
export function formatAlias(kind: string, value: string): string {
  return `${kind}:${value}`;
}

const UNITS: Record<string, bigint> = {
  ns: 1n,
  us: 1000n,
  'µs': 1000n, // U+00B5
  'μs': 1000n, // U+03BC
  ms: 1_000_000n,
  s: NS,
  m: 60n * NS,
  h: 3600n * NS,
};
const MAX = 1n << 63n;

/**
 * Reads a duration as Go's time.ParseDuration does ('1s', '500ms', '1m30s',
 * '1.5h') and returns nanoseconds. Throws on anything else.
 */
export function parseDuration(text: string): bigint {
  const bad = (why: string) => new Error(`hook: ${why} in duration ${JSON.stringify(text)}`);
  let s = text;
  let neg = false;
  if (s !== '' && (s[0] === '-' || s[0] === '+')) {
    neg = s[0] === '-';
    s = s.slice(1);
  }
  if (s === '0') return 0n;
  if (s === '') throw bad('nothing');
  const digit = (c: string | undefined) => c !== undefined && c >= '0' && c <= '9';
  let d = 0n;
  while (s !== '') {
    if (!(s[0] === '.' || digit(s[0]))) throw bad('no number');
    let i = 0;
    let v = 0n;
    while (digit(s[i])) {
      v = v * 10n + BigInt(s[i]);
      if (v > MAX) throw bad('overflow');
      i++;
    }
    const pre = i > 0;
    s = s.slice(i);
    let f = 0n;
    let scale = 1;
    let post = false;
    if (s[0] === '.') {
      s = s.slice(1);
      let j = 0;
      let overflow = false;
      while (digit(s[j])) {
        if (!overflow) {
          const y = f * 10n + BigInt(s[j]);
          if (y > MAX) overflow = true;
          else {
            f = y;
            scale *= 10;
          }
        }
        j++;
      }
      post = j > 0;
      s = s.slice(j);
    }
    if (!pre && !post) throw bad('no number');
    let k = 0;
    while (k < s.length && s[k] !== '.' && !digit(s[k])) k++;
    if (k === 0) throw bad('no unit');
    const unit = UNITS[s.slice(0, k)];
    if (unit === undefined) throw bad(`unknown unit ${JSON.stringify(s.slice(0, k))}`);
    s = s.slice(k);
    if (v > MAX / unit) throw bad('overflow');
    v *= unit;
    if (f > 0n) {
      v += BigInt(Math.trunc(Number(f) * (Number(unit) / scale)));
      if (v > MAX) throw bad('overflow');
    }
    d += v;
    if (d > MAX) throw bad('overflow');
  }
  if (neg) return -d;
  if (d > MAX - 1n) throw bad('overflow');
  return d;
}

// A precision given in code (milliseconds or a duration), or 0n for none.
function precisionOf(v: number | string | undefined): bigint {
  try {
    if (typeof v === 'number') return Number.isFinite(v) && v > 0 ? BigInt(Math.round(v * 1e6)) : 0n;
    if (typeof v === 'string' && v !== '') return parseDuration(v);
  } catch {
    // An unreadable precision keeps times as given, as in the Go hook.
  }
  return 0n;
}

type Attr = { key: string; value: { stringValue?: string; intValue?: string } };
type Span = { traceId: string; spanId: string; name: string; kind: number; startTimeUnixNano: string; endTimeUnixNano: string; attributes: Attr[] };

function spanId(): string {
  for (;;) {
    const b = randomBytes(8);
    if (!b.every((x) => x === 0)) return b.toString('hex');
  }
}

export class Hook {
  private readonly tenant: string;
  private readonly hop: string;
  private readonly endpoint: string;
  private readonly service: string;
  private readonly precisionNs: bigint;
  private readonly aliasURL: string;
  private readonly maxQueue: number;
  private queue: Span[] = [];
  private served = new Set<string>();
  private pairs = new Map<string, string>(); // kind:value → device
  private readonly timer: NodeJS.Timeout;
  private aliasTimer: NodeJS.Timeout | undefined;
  private aliasAbort: AbortController | undefined;
  private sending: Promise<void> = Promise.resolve();
  private busy = false;
  private closed = false;

  constructor(cfg: HookConfig) {
    const env = process.env;
    this.tenant = cfg.tenant || env.HOPWAKE_TENANT || '';
    this.hop = cfg.hop || '';
    if (this.tenant === '' || this.hop === '') throw new Error('hook: tenant and hop are required');
    this.endpoint = (cfg.endpoint || env.HOPWAKE_OTLP_ENDPOINT || 'http://localhost:4318').replace(/\/+$/, '');
    this.service = cfg.service || this.hop;
    const given = cfg.timePrecision !== undefined && cfg.timePrecision !== 0 && cfg.timePrecision !== '';
    this.precisionNs = given ? precisionOf(cfg.timePrecision) : precisionOf(env.HOPWAKE_TIME_PRECISION);
    this.aliasURL = (cfg.aliases || env.HOPWAKE_API || '').replace(/\/+$/, '');
    this.maxQueue = cfg.maxQueue ?? 50_000;
    this.timer = setInterval(() => {
      if (!this.busy) void this.flush();
    }, 1000);
    this.timer.unref();
    if (this.aliasURL !== '') this.followAliases();
  }

  /** Marks one reading with a sequence number. */
  mark(op: Op, device: string, seq: number | bigint, boot = ''): void {
    try {
      const id = journeyId(this.tenant, device, seq, boot);
      if (!this.firstServe(op, id)) return;
      this.push(op, id, device, BigInt.asIntN(64, BigInt(seq)).toString(), '', '');
    } catch {
      // A sequence number that is not one: the mark is dropped.
    }
  }

  /** mark for sources with a boot counter. */
  markBoot(op: Op, device: string, boot: string, seq: number | bigint): void {
    this.mark(op, device, seq, boot);
  }

  /** Marks a reading known by its device, message type and the time the device gave it. */
  markTime(op: Op, device: string, messageType: string, at: Date | bigint): void {
    this.timeMark(op, device, '', messageType, at);
  }

  /**
   * markTime at a hop that knows both the device and another name it goes by
   * further down, such as a system ID. Hopwake learns the pair from it.
   */
  markTimeTeach(op: Op, device: string, aliasKind: string, aliasValue: string, messageType: string, at: Date | bigint): void {
    this.timeMark(op, device, formatAlias(aliasKind, aliasValue), messageType, at);
  }

  /**
   * markTime for a hop that knows the device only by an alias. Returns false,
   * and marks nothing, when Hopwake has not learned the alias yet.
   */
  markTimeAlias(op: Op, aliasKind: string, aliasValue: string, messageType: string, at: Date | bigint): boolean {
    const alias = formatAlias(aliasKind, aliasValue);
    const device = this.pairs.get(alias);
    if (device === undefined) return false;
    this.timeMark(op, device, alias, messageType, at);
    return true;
  }

  /** Sends what is queued. */
  flush(): Promise<void> {
    const run = this.sending.then(() => this.sendQueued(Date.now() + CLOSE_WAIT_MS));
    this.sending = run;
    return run;
  }

  /** Sends what is queued, waiting at most a few seconds, and stops the hook. */
  async close(): Promise<void> {
    if (this.closed) return this.sending;
    this.closed = true;
    clearInterval(this.timer);
    clearTimeout(this.aliasTimer);
    this.aliasAbort?.abort();
    await this.flush();
  }

  private timeMark(op: Op, device: string, alias: string, messageType: string, at: Date | bigint): void {
    try {
      const id = timeJourneyId(this.tenant, device, messageType, at, this.precisionNs);
      if (!this.firstServe(op, id)) return;
      this.push(op, id, device, '0', messageType, alias);
    } catch {
      // A time that is not one: the mark is dropped.
    }
  }

  private firstServe(op: Op, id: string): boolean {
    if (op !== 'serve') return true;
    if (this.served.has(id)) return false;
    if (this.served.size >= SERVED_LIMIT) this.served.clear();
    this.served.add(id);
    return true;
  }

  private push(op: Op, id: string, device: string, seq: string, messageType: string, alias: string): void {
    if (this.closed || this.queue.length >= this.maxQueue) return; // Hopwake is away; drop rather than grow
    const now = (BigInt(Date.now()) * 1_000_000n).toString();
    const s = (key: string, v: string): Attr => ({ key, value: { stringValue: v } });
    const attributes: Attr[] = [
      s('hopwake.schema', 'hop/v1'),
      s('hopwake.tenant', this.tenant),
      s('hopwake.device.id', device),
      { key: 'hopwake.seq', value: { intValue: seq } },
      s('hopwake.journey.id', id),
      s('hopwake.hop', this.hop),
      s('hopwake.op', op),
      s('hopwake.correlation', 'derived'),
      { key: 'hopwake.payload.bytes', value: { intValue: '0' } },
      s('messaging.system', 'hook'),
    ];
    if (messageType !== '') attributes.push(s('hopwake.message.type', messageType));
    if (alias !== '') attributes.push(s('hopwake.device.alias', alias));
    this.queue.push({
      traceId: id,
      spanId: spanId(),
      name: `hop ${this.hop}`,
      kind: 5, // consumer
      startTimeUnixNano: now,
      endTimeUnixNano: now,
      attributes,
    });
  }

  private async sendQueued(deadline: number): Promise<void> {
    this.busy = true;
    try {
      while (this.queue.length > 0 && Date.now() < deadline) {
        // A batch that fails is lost; the rest waits for the next tick.
        if (!(await this.send(this.queue.splice(0, BATCH), deadline))) break;
      }
    } finally {
      this.busy = false;
    }
  }

  private async send(spans: Span[], deadline: number): Promise<boolean> {
    const body = {
      resourceSpans: [{
        resource: { attributes: [{ key: 'service.name', value: { stringValue: this.service } }] },
        scopeSpans: [{ scope: { name: 'hopwake/hook' }, spans }],
      }],
    };
    const stop = new AbortController();
    const timer = setTimeout(() => stop.abort(), Math.max(0, Math.min(SEND_TIMEOUT_MS, deadline - Date.now())));
    timer.unref();
    try {
      const res = await fetch(`${this.endpoint}/v1/traces`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
        signal: stop.signal,
      });
      await res.arrayBuffer();
      return res.ok;
    } catch {
      return false; // Hopwake is not reachable: these marks are lost, the service carries on.
    } finally {
      clearTimeout(timer);
    }
  }

  // Fetches the alias list now and every 5 seconds; a failed fetch keeps the copy.
  private followAliases(): void {
    const stop = new AbortController();
    this.aliasAbort = stop;
    const timer = setTimeout(() => stop.abort(), SEND_TIMEOUT_MS);
    timer.unref();
    fetch(`${this.aliasURL}/api/aliases`, { signal: stop.signal })
      .then(async (res) => {
        if (!res.ok) {
          await res.arrayBuffer();
          return;
        }
        const list: unknown = await res.json();
        if (!Array.isArray(list)) return;
        const pairs = new Map<string, string>();
        for (const p of list) {
          if (p && typeof p.kind === 'string' && typeof p.value === 'string' && typeof p.device === 'string') {
            pairs.set(formatAlias(p.kind, p.value), p.device);
          }
        }
        this.pairs = pairs;
      })
      .catch(() => {})
      .finally(() => {
        clearTimeout(timer);
        if (this.closed) return;
        this.aliasTimer = setTimeout(() => this.followAliases(), ALIAS_EVERY_MS);
        this.aliasTimer.unref();
      });
  }
}
