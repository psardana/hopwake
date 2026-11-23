#!/usr/bin/env bash
# Phase 1 demo: Hopwake watches the reference pipeline from the outside and
# names the hop where a device's data stops, in the journey view.
#
#   scripts/demo-phase-1.sh             walk through it, pausing between steps
#   scripts/demo-phase-1.sh --no-pause  run straight through
#   scripts/demo-phase-1.sh --stop      stop everything
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
wait_key() { if [ "$PAUSE" = 1 ]; then read -r -p "Press Enter to continue… " _; fi; }
verdict() { curl -s "localhost:8090/api/devices/$1" | python3 -c 'import sys,json; print("  " + json.load(sys.stdin)["verdict"]["sentence"])'; }
wait_for() { # wait_for <seconds> <command…>: retry until it succeeds
  local limit=$1; shift
  for _ in $(seq 1 "$limit"); do "$@" >/dev/null 2>&1 && return 0; sleep 1; done
  echo "Timed out waiting for: $*" >&2; return 1
}

docker info >/dev/null 2>&1 || { echo "Docker is not running." >&2; exit 1; }

# 48 devices run; the fleet list expects 50, so two never report.
export DEVICES=48 INTERVAL=1s

step "1. Start the reference pipeline (48 devices running, 50 expected) and Hopwake next to it"
echo "Hopwake reads copies only: an MQTT subscription and its own Kafka consumer group."
"${HOP[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
"${REF[@]}" --profile tracing down -v --remove-orphans >/dev/null 2>&1 || true
"${REF[@]}" up -d --build --quiet-pull 2>&1 | grep -E 'Error|error' || true
"${HOP[@]}" up -d --build --quiet-pull 2>&1 | grep -E 'Error|error' || true
printf 'Waiting for healthy devices in the journey view'
wait_for 300 sh -c "curl -s localhost:8090/api/fleet | grep -q '\"state\":\"ok\"'"
echo
echo "Open http://localhost:8090 — the fleet, problems first: dev-00048 and dev-00049 have never reported."
wait_key

step "2. Break two devices while everything runs"
echo "dev-00042: dropped at the bridge, silently."
echo "dev-00043: its temp_c field renamed at the normaliser, as an unannounced firmware change would."
start=$(date +%s)
BRIDGE_FAULT_DEVICES=dev-00042 BRIDGE_FAULT_DROP=1 \
NORMALISER_FAULT_DEVICES=dev-00043 NORMALISER_FAULT_RENAME=1 \
  "${REF[@]}" up -d --no-deps bridge normaliser 2>&1 | grep -E 'Error' || true
echo "Restarting the two services to change their settings drops a few readings for every device:"
echo "their MQTT session does not survive a restart. Hopwake sees that loss too, for about 15 seconds."
# Wait for the verdicts that name the right hops, not just any stop.
wait_for 180 sh -c "curl -s localhost:8090/api/devices/dev-00042 | grep -q '\"stopped_after\":\"mqtt:plant/+/+/telemetry\"'"
wait_for 180 sh -c "curl -s localhost:8090/api/devices/dev-00043 | grep -q '\"stopped_after\":\"kafka:telemetry.raw\"'"
wait_for 180 sh -c "curl -s localhost:8090/api/devices/dev-00044 | grep -q '\"state\":\"ok\"'"
echo "Both named at the right hop $(( $(date +%s) - start )) seconds after the faults started; the rest of the fleet is healthy again."
wait_key

step "3. Where did their data stop? The journey service's verdicts"
for d in dev-00042 dev-00043 dev-00044 dev-00048; do verdict "$d"; done
echo
echo "In the view: http://localhost:8090/#/device/dev-00042 — the route, the last hop reached, and each reading's dots."
wait_key

step "4. Check against what really happened: the fault log"
for hop in bridge normaliser; do
  "${REF[@]}" logs --no-log-prefix "$hop" 2>/dev/null | grep '"fault"' | tail -1
done

bold "Done. Stop everything with: scripts/demo-phase-1.sh --stop"
