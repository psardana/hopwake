#!/usr/bin/env bash
# Phase 0 demo: the reference pipeline, a device that silently stops, and the
# journey probe that finds where it stopped.
#
#   scripts/demo-phase-0.sh             walk through it, pausing between steps
#   scripts/demo-phase-0.sh --no-pause  run straight through
#   scripts/demo-phase-0.sh --tracing   also start Jaeger with stock OpenTelemetry
#   scripts/demo-phase-0.sh --stop      stop everything
set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE=(docker compose -f reference/compose.yaml)
PAUSE=1
TRACING=0
for arg in "$@"; do
  case "$arg" in
    --no-pause) PAUSE=0 ;;
    --tracing) TRACING=1 ;;
    --stop) "${COMPOSE[@]}" --profile tracing down; exit 0 ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done
if [ "$TRACING" = 1 ]; then COMPOSE+=(--env-file reference/tracing.env); fi

bold() { printf '\n\033[1m%s\033[0m\n' "$*"; }
step() { bold "── $*"; }
wait_key() { if [ "$PAUSE" = 1 ]; then read -r -p "Press Enter to continue… " _; fi; }

docker info >/dev/null 2>&1 || { echo "Docker is not running." >&2; exit 1; }
command -v go >/dev/null || { echo "Go is needed for the journey probe." >&2; exit 1; }

export DEVICES=${DEVICES:-50} INTERVAL=${INTERVAL:-1s}

step "1. Start the reference pipeline: ${DEVICES} devices, one reading per second each"
echo "devices → Mosquitto → bridge → Kafka → normaliser → aggregator → recorder → Postgres → API → portal"
# Start from empty volumes: leftovers from an interrupted run stop Kafka and Postgres.
"${COMPOSE[@]}" --profile tracing down -v --remove-orphans >/dev/null 2>&1 || true
"${COMPOSE[@]}" up -d --build --quiet-pull 2>&1 | grep -E 'Error|error' || true
printf 'Waiting for the first closed minute to reach the portal'
for i in $(seq 1 60); do
  [ "$(curl -s localhost:8080/api/sites | tr -cd '{' | wc -c | tr -d ' ')" -gt 0 ] 2>/dev/null && break
  if [ "$i" = 60 ]; then
    echo; echo "No data after 5 minutes. Check: docker compose -f reference/compose.yaml ps -a" >&2; exit 1
  fi
  printf '.'; sleep 5
done
echo
echo "Portal: http://localhost:8080 — four sites, every device reporting."
wait_key

step "2. Break two devices on purpose"
echo "dev-00042: dropped at the bridge, silently."
echo "dev-00043: its temp_c field renamed at the normaliser, as an unannounced firmware change would."
BRIDGE_FAULT_DEVICES=dev-00042 BRIDGE_FAULT_DROP=1 \
NORMALISER_FAULT_DEVICES=dev-00043 NORMALISER_FAULT_RENAME=1 \
  "${COMPOSE[@]}" up -d --no-deps bridge normaliser 2>&1 | grep -E 'Started|Error' || true
echo "Give it a minute, then look at site-02 in the portal: both devices turn orange, the other 48 carry on."
wait_key

step "3. Ask where their data stopped: the journey probe"
echo "It watches MQTT and telemetry.normalized, derives each reading's journey ID on both sides, and needs no headers."
go build -o bin/journeyprobe ./cmd/journeyprobe
./bin/journeyprobe -every 12s & PROBE=$!
sleep 40
kill "$PROBE" 2>/dev/null || true
wait "$PROBE" 2>/dev/null || true
wait_key

step "4. Check against what really happened: the fault log"
for hop in bridge normaliser; do
  "${COMPOSE[@]}" logs --no-log-prefix "$hop" 2>/dev/null | grep '"fault"' | tail -2
done

if [ "$TRACING" = 1 ]; then
  step "5. The same question in Jaeger (stock OpenTelemetry)"
  echo "Open http://localhost:16686, service 'bridge', tags device.id=dev-00042."
  echo "Every trace is one span with status OK: nothing says the data was lost."
fi

bold "Done. Stop everything with: scripts/demo-phase-0.sh --stop"
