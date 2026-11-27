// The hook conformance script (docs/hooks.md) with the Node hook. Run it
// under hookcheck, from the repository root:
//
//   go build -o bin/hookcheck ./cmd/hookcheck
//   bin/hookcheck -service conformance-node -- node --experimental-strip-types pkg/hook/node/conformance.ts
import { Hook } from './hopwake-hook.ts';

function fail(why: string): never {
  console.error(`conformance: ${why}`);
  process.exit(1);
}

const h = new Hook({ tenant: process.env.HOPWAKE_TENANT, hop: process.env.HOPWAKE_HOP ?? '', service: 'conformance-node' });
const at = new Date('2026-11-18T10:00:00.750Z');
const typ = 'ems.measurements.Measurement';

h.mark('store', 'dev-00042', 1683);
h.markBoot('store', 'dev-00042', '7', 12);
h.mark('serve', 'dev-00042', 1683);
h.mark('serve', 'dev-00042', 1683); // the same reading served again: not sent
h.markTime('store', 'EMS0000042', typ, at);
h.markTimeTeach('store', 'EMS0000042', 'systemId', '100042', typ, at);
// The alias list arrives shortly after the hook starts.
const deadline = Date.now() + 15_000;
while (!h.markTimeAlias('serve', 'systemId', '100042', typ, at)) {
  if (Date.now() > deadline) fail('systemId 100042 never became known');
  await new Promise((r) => setTimeout(r, 100));
}
if (h.markTimeAlias('serve', 'systemId', '999999', typ, at)) fail('an unknown alias was marked');
await h.close();
