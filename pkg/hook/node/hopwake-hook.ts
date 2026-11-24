// Hopwake hook for Node services: lets a service say that it stored or
// served readings, for hops no tap can watch from outside.
//
//   const served = new Hook({ tenant: 'reference', hop: 'hook:api.served' });
//   served.mark('serve', 'dev-00042', 1683);
//
// Marks are hop events sent as OTLP/JSON spans to Hopwake's collector, keyed
// by the same journey ID a tap derives. They are batched, sent at most once a
// second, and dropped quietly when Hopwake cannot be reached: the service is
// never slowed or stopped by its hook. A reading is marked served only the
// first time, however often it is asked for.
import { createHash, randomBytes } from 'node:crypto';

export type Op = 'store' | 'serve';

export interface HookConfig {
  tenant: string;
  hop: string; // matches a hook source in the rules file: "hook:" + match
  endpoint?: string; // default $HOPWAKE_OTLP_ENDPOINT, else http://localhost:4318
  service?: string;
}

function field(s: string): Buffer {
  const b = Buffer.from(s, 'utf8');
  const n = Buffer.alloc(4);
  n.writeUInt32BE(b.length);
  return Buffer.concat([n, b]);
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
  const id = h.digest().subarray(0, 16);
  if (id.every((x) => x === 0)) id[15] = 1;
  return id.toString('hex');
}

type Attr = { key: string; value: { stringValue?: string; intValue?: string } };
type Span = { traceId: string; spanId: string; name: string; kind: number; startTimeUnixNano: string; endTimeUnixNano: string; attributes: Attr[] };

export class Hook {
  private readonly cfg: Required<HookConfig>;
  private queue: Span[] = [];
  private served = new Set<string>();
  private readonly timer: NodeJS.Timeout;

  constructor(cfg: HookConfig) {
    this.cfg = {
      endpoint: process.env.HOPWAKE_OTLP_ENDPOINT || 'http://localhost:4318',
      service: cfg.hop,
      ...cfg,
    };
    this.timer = setInterval(() => void this.flush(), 1000);
    this.timer.unref();
  }

  /** Marks one reading. */
  mark(op: Op, device: string, seq: number | bigint, boot = ''): void {
    const id = journeyId(this.cfg.tenant, device, seq, boot);
    if (op === 'serve') {
      if (this.served.has(id)) return;
      if (this.served.size > 100_000) this.served.clear();
      this.served.add(id);
    }
    if (this.queue.length >= 50_000) return; // Hopwake is away; drop rather than grow
    const now = (BigInt(Date.now()) * 1_000_000n).toString();
    const s = (key: string, v: string): Attr => ({ key, value: { stringValue: v } });
    this.queue.push({
      traceId: id,
      spanId: randomBytes(8).toString('hex'),
      name: `hop ${this.cfg.hop}`,
      kind: 5, // consumer
      startTimeUnixNano: now,
      endTimeUnixNano: now,
      attributes: [
        s('hopwake.schema', 'hop/v1'),
        s('hopwake.tenant', this.cfg.tenant),
        s('hopwake.device.id', device),
        { key: 'hopwake.seq', value: { intValue: BigInt(seq).toString() } },
        s('hopwake.journey.id', id),
        s('hopwake.hop', this.cfg.hop),
        s('hopwake.op', op),
        s('hopwake.correlation', 'derived'),
        { key: 'hopwake.payload.bytes', value: { intValue: '0' } },
        s('messaging.system', 'hook'),
      ],
    });
  }

  /** Sends what is queued. */
  async flush(): Promise<void> {
    if (this.queue.length === 0) return;
    const spans = this.queue.splice(0, this.queue.length);
    const body = {
      resourceSpans: [{
        resource: { attributes: [{ key: 'service.name', value: { stringValue: this.cfg.service } }] },
        scopeSpans: [{ scope: { name: 'hopwake/hook' }, spans }],
      }],
    };
    try {
      await fetch(`${this.cfg.endpoint}/v1/traces`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(3000),
      });
    } catch {
      // Hopwake is not reachable: these marks are lost, the service carries on.
    }
  }

  async close(): Promise<void> {
    clearInterval(this.timer);
    await this.flush();
  }
}
