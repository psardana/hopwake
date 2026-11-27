#!/usr/bin/env bash
# Phase 4 demo: hooks in every language, devices that mark their own sends,
# and a tap that needs only read rights.
#
#   scripts/demo-phase-4.sh             walk through it, pausing between steps
#   scripts/demo-phase-4.sh --no-pause  run straight through
#   scripts/demo-phase-4.sh --stop      stop everything
#
# The energy-storage scenario (scenarios/ems) is local to one machine and not part of
# this repository. When it is there, step 4 runs it: the same path with a
# service in each language and devices that mark through the C hook.
set -uo pipefail

cd "$(dirname "$0")/.."
QA=(docker compose -f reference/qa/compose.yaml)
PAUSE=1
for arg in "$@"; do
  case "$arg" in
    --no-pause) PAUSE=0 ;;
    --stop) "${QA[@]}" --profile two-taps down -v; [ -x scenarios/ems/demo.sh ] && scenarios/ems/demo.sh --stop; exit 0 ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done

bold() { printf '\n\033[1m%s\033[0m\n' "$*"; }
step() { bold "── $*"; }
wait_key() { if [ "$PAUSE" = 1 ]; then read -r -p "Press Enter to continue… " _; elif [ -n "${DEMO_STEP_GAP:-}" ]; then sleep "$DEMO_STEP_GAP"; fi; }

step "1. One hook contract, seven languages"
echo "Every hook computes the shared test vectors and passes the wire check against cmd/hookcheck."
scripts/check-hooks.sh 2>&1 | grep -E '^── |all 6 marks|every hook|FAILED|device marks: all'
wait_key

step "2. A device marks its own sends, in C"
echo "The C hook writes a mark message; the tap's reader turns it into the reading's journey ID."
make -s -C pkg/hook/c build/emit >/dev/null && pkg/hook/c/build/emit 2 \
  time ems.sent send ems.measurements.Measurement 1763460000750 1763460000900 \
  time ems.sent store ems.measurements.Measurement 1763460005750 0 \
  time ems.sent send ems.measurements.Measurement 1763460010750 0 | sed 's/^/  /'
echo "A queue of two kept the newest two marks and reports the one it dropped."
wait_key

step "3. A tap with read rights only, on a locked-down broker and cluster"
STEP_SECONDS=${STEP_SECONDS:-30} scripts/qa-tap.sh --rights
wait_key

if [ -x scenarios/ems/demo.sh ]; then
  step "4. The energy-storage measurement path, with a service in each language"
  if [ "$PAUSE" = 1 ]; then scenarios/ems/demo.sh; else scenarios/ems/demo.sh --no-pause; fi
else
  step "4. (The energy-storage scenario is not on this machine; skipped.)"
fi

bold "Done. Load numbers: scripts/qa-tap.sh (about 12 minutes). Stop everything with: scripts/demo-phase-4.sh --stop"
