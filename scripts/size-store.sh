#!/usr/bin/env bash
# Measures what Hopwake's store takes at rising message rates: hop events
# written, bytes on disk per hop event, and the CPU and memory of the tap,
# the collector and ClickHouse. It runs the reference/qa broker and cluster
# with simulated devices, and Hopwake's own stack beside them.
#
#   scripts/size-store.sh                 steps of 1,000, 5,000 and 10,000 msg/s
#   RATES="1000 2000" STEP_SECONDS=120 scripts/size-store.sh
#
# Prints a Markdown table; the readings are JSON, two hops each (MQTT, Kafka).
set -uo pipefail
cd "$(dirname "$0")/.."
QA=(docker compose -f reference/qa/compose.yaml)
HOP=(docker compose -f deploy/compose.yaml)
export HOPWAKE_RULES=../reference/qa/rules.yaml HOPWAKE_NETWORK=hopwake-qa_default
STEP=${STEP_SECONDS:-120}
WARM=${WARM_SECONDS:-40}
RATES=${RATES:-1000 5000 10000}
ch() { docker exec hopwake-clickhouse-1 clickhouse-client -q "$1"; }
rows() { ch "SELECT sum(rows) FROM system.parts WHERE active AND database = 'hopwake' AND table = '$1'"; }

echo "Starting the broker, the cluster and Hopwake"
"${HOP[@]}" down -v >/dev/null 2>&1
DEVICES=100 INTERVAL=2s "${QA[@]}" up -d --build --wait mosquitto kafka kafka-setup bridge devices >/dev/null 2>&1 || true
"${HOP[@]}" up -d --build collector clickhouse journey tap >/dev/null 2>&1
until curl -sf localhost:8090/api/fleet >/dev/null; do sleep 2; done

out=()
for rate in $RATES; do
  devices=$(( rate * 2 ))
  echo "── $rate msg/s: $devices devices every 2 s"
  DEVICES=$devices INTERVAL=2s CONNS=$(( devices / 1000 + 10 )) "${QA[@]}" up -d --force-recreate devices >/dev/null 2>&1
  sleep "$WARM"
  h0=$(rows hop_events); r0=$(rows otel_traces); t0=$(date +%s)
  cpu_tap=0; cpu_col=0; cpu_ch=0; mem_ch=0; n=0
  while [ $(( $(date +%s) - t0 )) -lt "$STEP" ]; do
    while read -r name cpu mem; do
      cpu=${cpu%\%}; mem=${mem%MiB*}; mem=${mem%GiB*}
      case $name in
        hopwake-tap-1) cpu_tap=$(echo "$cpu_tap + $cpu" | bc) ;;
        hopwake-collector-1) cpu_col=$(echo "$cpu_col + $cpu" | bc) ;;
        hopwake-clickhouse-1) cpu_ch=$(echo "$cpu_ch + $cpu" | bc); mem_ch=$mem ;;
      esac
    done < <(docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' hopwake-tap-1 hopwake-collector-1 hopwake-clickhouse-1)
    n=$((n + 1)); sleep 5
  done
  secs=$(( $(date +%s) - t0 ))
  h1=$(rows hop_events); r1=$(rows otel_traces)
  out+=("$(python3 -c "
secs=$secs; n=max($n,1)
print('| %s | %s | %s | %.0f%% | %.0f%% | %.0f%% | %s |' % ('{:,}'.format($rate), '{:,}'.format(round(($h1-$h0)/secs)), '{:,}'.format(round(($r1-$r0)/secs)), $cpu_tap/n, $cpu_col/n, $cpu_ch/n, '$mem_ch MiB'))")")
  echo "${out[${#out[@]}-1]}"
done

echo "── Bytes on disk per row, after merging"
"${QA[@]}" stop devices >/dev/null 2>&1
sleep 10
for t in otel_traces otel_traces_trace_id_ts hop_events hop_last_seen device_counters tap_health; do ch "OPTIMIZE TABLE hopwake.$t FINAL" >/dev/null 2>&1; done
ch "SELECT table, sum(rows) AS row_count, round(sum(bytes_on_disk) / sum(rows), 1) AS bytes_per_row, formatReadableSize(sum(bytes_on_disk)) AS disk
    FROM system.parts WHERE active AND database = 'hopwake' GROUP BY table ORDER BY sum(bytes_on_disk) DESC FORMAT Markdown"
echo
echo "Cores: $(sysctl -n hw.ncpu 2>/dev/null || nproc). ${STEP}s per step after ${WARM}s warm-up. CPU is % of one core."
echo
echo "| Readings/s | Hop events written/s | Raw spans written/s | Tap CPU | Collector CPU | ClickHouse CPU | ClickHouse memory |"
echo "| --- | --- | --- | --- | --- | --- | --- |"
printf '%s\n' "${out[@]}"
