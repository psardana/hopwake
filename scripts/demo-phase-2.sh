#!/usr/bin/env bash
# Phase 2 demo: Hopwake says why a device's data stopped, and follows it all
# the way to the portal.
#
#   scripts/demo-phase-2.sh             walk through it, pausing between steps
#   scripts/demo-phase-2.sh --no-pause  run straight through
#   scripts/demo-phase-2.sh --stop      stop everything
set -euo pipefail

cd "$(dirname "$0")/.."
REF=(docker compose -f reference/compose.yaml)
HOP=(docker compose -f deploy/compose.yaml)
PAUSE=1
for arg in "$@"; do
  case "$arg" in
    --no-pause) PAUSE=0 ;;
    --stop) "${HOP[@]}" down -v; "${REF[@]}" --profile tracing down -v; exit 0 ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done

bold() { printf '\n\033[1m%s\033[0m\n' "$*"; }
step() { bold "── $*"; }
# DEMO_STEP_GAP=<seconds> holds each step that long when running with --no-pause,
# for example while recording the demo.
wait_key() { if [ "$PAUSE" = 1 ]; then read -r -p "Press Enter to continue… " _; elif [ -n "${DEMO_STEP_GAP:-}" ]; then sleep "$DEMO_STEP_GAP"; fi; }
api() { curl -sf "localhost:8090$1"; }
say() { python3 -c "import sys,json; d=json.load(sys.stdin); $1"; }
verdict() { api "/api/devices/$1" | say 'print("  " + d["verdict"]["sentence"])'; }
wait_for() { # wait_for <seconds> <command…>: retry until it succeeds
  local limit=$1; shift
  for _ in $(seq 1 "$limit"); do "$@" >/dev/null 2>&1 && return 0; sleep 1; done
  echo "Timed out waiting for: $*" >&2; return 1
}
has() { api "$1" | grep -q "$2"; }

docker info >/dev/null 2>&1 || { echo "Docker is not running." >&2; exit 1; }

# 48 devices run; the fleet list expects 50. Faults, all set before start:
#   dev-00042  the bridge drops its readings, 150 s in
#   dev-00043  new firmware renames temp_c to temperature, 150 s in
#   dev-00044  the normaliser converts its temperature with 272.65, not 273.15
#   dev-00045  its firmware sends pressure_kpa as text
export DEVICES=48 INTERVAL=1s
export BRIDGE_FAULT_DEVICES=dev-00042 BRIDGE_FAULT_DROP=1 BRIDGE_FAULT_AFTER=150s
export FIRMWARE_RENAME_DEVICES=dev-00043 FIRMWARE_RENAME_AFTER=150s
export NORMALISER_FAULT_KELVIN_DEVICES=dev-00044
export FIRMWARE_TEXT_PRESSURE_DEVICES=dev-00045
# The recorder and the API mark readings stored and served; the normaliser
# sends its own spans to Hopwake's ingest.
export HOPWAKE_OTLP_ENDPOINT=http://collector:4318
export NORMALISER_OTEL_ENDPOINT=http://ingest:4319 NORMALISER_OTEL_DISABLED=false

step "1. Start the reference pipeline and Hopwake next to it"
echo "Four faults are set to happen; two of them start 150 seconds in."
"${HOP[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
"${REF[@]}" --profile tracing down -v --remove-orphans >/dev/null 2>&1 || true
"${REF[@]}" up -d --build --quiet-pull 2>&1 | grep -E 'Error|error' || true
start=$(date +%s)
"${HOP[@]}" up -d --build --quiet-pull 2>&1 | grep -E 'Error|error' || true
printf 'Waiting for healthy devices in the journey view'
wait_for 300 has /api/fleet '"state":"ok"'
echo
echo "Open http://localhost:8090"
wait_key

step "2. Watch three devices: full payloads at every hop, for an hour"
for d in dev-00010 dev-00044 dev-00045; do
  curl -sf -X POST -H 'X-Hopwake-User: demo' "localhost:8090/api/devices/$d/watch" | say 'print("  watching " + d["device"] + " until " + d["until"])'
done
echo "Payloads are redacted in the tap (the rules file's redact list) and kept 24 hours; nothing else leaves the tap."
wait_key

step "3. A declared schema catches what the pipeline lets through"
wait_for 120 has /api/devices/dev-00045 '"invalid_reason"'
wait_for 120 has /api/devices/dev-00045 'reach every hop'
api /api/devices/dev-00045 | say '
h=[x for x in d["hops"] if x["invalid"]]
print("  " + d["verdict"]["sentence"])
for x in h: print("  at %s: %d readings broke the declared schema: %s" % (x["hop"], x["invalid"], x["invalid_reason"]))'
echo "The normaliser parses the text and passes it on: the pipeline is healthy, the data is not what was declared."
wait_key

step "4. A confirmed mapping catches a wrong conversion"
wait_for 120 has /api/devices/dev-00044 '"findings":\[{'
api /api/devices/dev-00044 | say '
f=d["findings"][0]; e=f["example"]
print("  The %s turned %s %s into %s %s (seq %d); the confirmed mapping %s = %s expects %s. %d of %d watched readings." % (f["via"], e["expr"].split()[0], e["in"], e["field"], e["got"], f["seq"], e["field"], e["expr"], e["want"], f["broken"], f["checked"]))'
echo "Mappings, proposed from watched devices' paired readings: http://localhost:8090/#/mappings"
api /api/mappings | say '
e=[x for x in d["edges"] if x["to"].endswith("normalized")][0]
for r in e["relations"]: print("  %-15s = %-22s %s%s" % (r["out"], r["expr"], r["status"], (", not " + ", ".join(r["outliers"])) if r.get("partial") else ""))'
wait_key

step "5. 150 seconds in: firmware changes on dev-00043, the bridge starts dropping dev-00042"
left=$(( 150 - ($(date +%s) - start) ))
[ "$left" -gt 0 ] && echo "Waiting ${left}s for the faults to start…"
wait_for 400 has /api/devices/dev-00043 '"reason":"no field temp_c"'
wait_for 400 has /api/devices/dev-00042 '"stopped_at":"bridge"'
verdict dev-00043
verdict dev-00042
echo "The device page shows dev-00043's shape change at MQTT and raw: temp_c removed, temperature added."
wait_key

step "6. The site medians: which devices went in, and who is missing"
wait_for 180 has /api/devices/dev-00042 '"missing":\[{'
api "/api/windows?key=site-02" | say '
w=[x for x in d["windows"] if x["missing"]][0]
print("  site-02 %s, %s: %d readings from %d devices; missing: %s" % (w["op"], w["start"][11:16], w["inputs"], w["device_count"], ", ".join(w["missing"])))'
echo "Aggregates: http://localhost:8090/#/aggregates"
wait_key

step "7. Past Kafka: stored by the recorder, served by the API"
curl -sf 'localhost:8080/api/devices/dev-00010/readings?limit=20' >/dev/null
wait_for 30 has /api/devices/dev-00010 '"hop":"hook:api.served"'
api /api/devices/dev-00010 | say '
for x in d["hops"]: print("  %-28s %5d readings" % (x["hop"], x["count"]))'
echo "stored and served come from hooks in the recorder and the API; span:normaliser.process from the"
echo "normaliser's own tracing, through Hopwake's ingest. served is optional: only on request."
wait_key

step "8. When the tap itself misses readings"
echo "Pausing the tap for 75 seconds, as if it fell over: long enough that devices would look silent."
docker pause hopwake-tap-1 >/dev/null
sleep 75
api /api/fleet | say '
from collections import Counter
names = {"unsure": "waiting for the tap", "unknown": "never seen", "ok": "healthy"}
print("  " + ", ".join("%d %s" % (n, names.get(s, s)) for s, n in Counter(x["state"] for x in d["devices"]).items()))'
verdict dev-00010
docker unpause hopwake-tap-1 >/dev/null
echo "Unpaused. Verdicts come back once the tap has caught up:"
wait_for 120 has /api/devices/dev-00010 '"state":"ok"'
verdict dev-00010
verdict dev-00042
wait_key

step "9. Check against what really happened: the fault logs"
for svc in bridge normaliser; do
  "${REF[@]}" logs --no-log-prefix "$svc" 2>/dev/null | grep '"fault"' | tail -1
done

bold "Done. Stop everything with: scripts/demo-phase-2.sh --stop"
