#!/usr/bin/env bash
# Runs the read-only tap rig (reference/qa): checks that the tap's users can
# read but not write, then measures how much one tap reads, and two taps
# splitting the load, at rising message rates.
#
#   scripts/qa-tap.sh              everything (about 12 minutes)
#   scripts/qa-tap.sh --rights     only the rights checks
#   STEP_SECONDS=60 scripts/qa-tap.sh
#
# Prints a Markdown table at the end. The numbers depend on the machine:
# they come with its core count and load average.
set -uo pipefail
cd "$(dirname "$0")/.."
QA=(docker compose -f reference/qa/compose.yaml)
STEP=${STEP_SECONDS:-60}
WARM=${WARM_SECONDS:-20}

say() { printf '\n\033[1m── %s\033[0m\n' "$*"; }
stats() { docker exec hopwake-qa-sink-1 wget -qO- "localhost:4318/stats${1:-}"; }

say "Starting the rig"
DEVICES=200 INTERVAL=2s "${QA[@]}" up -d --build --wait mosquitto kafka sink >/dev/null 2>&1 || "${QA[@]}" up -d --build
DEVICES=200 INTERVAL=2s "${QA[@]}" up -d --build >/dev/null 2>&1

say "What the tap's users may do"
mqtt=$(docker exec hopwake-qa-mosquitto-1 mosquitto_pub -h mosquitto -p 8883 --cafile /certs/ca.pem \
  -u hopwake-tap -P qa-tap-password -V 5 -q 1 -t plant/s1/dev-x/telemetry -m '{}' -d 2>&1 | grep -o 'RC:[0-9]*')
[ "$mqtt" = "RC:135" ] && echo "  MQTT: publishing as hopwake-tap is refused (PUBACK reason 135, not authorized)" \
                       || echo "  MQTT: UNEXPECTED, publish answered $mqtt"
kafka=$(docker exec hopwake-qa-kafka-1 sh -c '
  printf "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=SCRAM-SHA-512\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"hopwake-tap\" password=\"qa-tap-password\";\n" > /tmp/tap.properties
  echo x | timeout 40 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29093 --producer.config /tmp/tap.properties --topic telemetry.raw 2>&1 | grep -oE "[A-Za-z]+AuthorizationException" | head -1
  timeout 40 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29093 --consumer.config /tmp/tap.properties --topic telemetry.raw --group someone-else --max-messages 1 2>&1 | grep -oE "[A-Za-z]+AuthorizationException" | head -1')
echo "$kafka" | sed -n 1p | sed 's/^/  Kafka: producing as hopwake-tap is refused: /'
echo "$kafka" | sed -n 2p | sed 's/^/  Kafka: joining another consumer group is refused: /'
docker logs hopwake-qa-tap-1 2>&1 | grep -E "without a consumer group|mqtt: watching" | tail -2 | sed 's/^[0-9/: ]*/  tap: /'
[ "${1:-}" = "--rights" ] && exit 0

rows=()
# step NAME DEVICES INTERVAL SAMPLE TAPS
step() {
  local name=$1 devices=$2 interval=$3 sample=$4 taps=$5
  say "$name: $devices devices every $interval, sample 1 in $sample, $taps tap(s)"
  local share="" group=none profile=()
  if [ "$taps" = 2 ]; then share=hopwake; group=hopwake-tap; profile=(--profile two-taps); fi
  DEVICES=$devices INTERVAL=$interval TAP_SAMPLE=$sample TAP_MQTT_SHARE=$share TAP_GROUP=$group \
    "${QA[@]}" "${profile[@]}" up -d --force-recreate devices tap $( [ "$taps" = 2 ] && echo tap-b ) >/dev/null 2>&1
  [ "$taps" = 1 ] && docker rm -f hopwake-qa-tap-b-1 >/dev/null 2>&1
  sleep "$WARM"
  local a b
  a=$(stats '?reset_delay=1'); local t0=$(date +%s)
  local cpu=0 mem=0 n=0
  while [ $(( $(date +%s) - t0 )) -lt "$STEP" ]; do
    for c in hopwake-qa-tap-1 $( [ "$taps" = 2 ] && echo hopwake-qa-tap-b-1 ); do
      read -r c1 m1 < <(docker stats --no-stream --format '{{.CPUPerc}} {{.MemUsage}}' "$c" | sed 's/%//; s/MiB.*//; s/GiB.*/000/')
      cpu=$(echo "$cpu + ${c1:-0}" | bc); mem=$(echo "if (${m1:-0} > $mem) ${m1:-0} else $mem" | bc)
    done
    n=$((n + 1)); sleep 5
  done
  b=$(stats); local secs=$(( $(date +%s) - t0 ))
  local load; load=$(sysctl -n vm.loadavg | awk '{print $2}')
  python3 - "$a" "$b" "$secs" "$name" "$devices" "$interval" "$sample" "$taps" "$cpu" "$n" "$mem" "$load" <<'PY'
import json, sys
a, b = json.loads(sys.argv[1]), json.loads(sys.argv[2])
secs, name, devices, interval, sample, taps = float(sys.argv[3]), *sys.argv[4:9]
cpu, n, mem, load = float(sys.argv[9]), int(sys.argv[10]), float(sys.argv[11]), sys.argv[12]
iv = float(interval.rstrip("s"))
d = lambda m, k: (b.get(m, {}).get(k, 0) - a.get(m, {}).get(k, 0)) / secs
mq, kf = "mqtt:plant/+/+/telemetry", "kafka:telemetry.raw"
spans = (b["spans"] - a["spans"]) / secs
row = f"| {name} | {int(devices)/iv:,.0f} | {d('tap_seen', mq):,.0f} | {d('tap_missed', mq):,.0f} | {d('tap_seen', kf):,.0f} | {b['tap_max_delay_ms'].get(kf, 0):,} / {b['tap_max_delay_ms'].get(mq, 0):,} | {spans:,.0f} | {cpu / max(n, 1):.0f}% | {mem:.0f} MiB | {load} |"
print(row)
open("/tmp/hopwake-qa-rows", "a").write(row + "\n")
PY
}

rm -f /tmp/hopwake-qa-rows
step "1k" 2000 2s 1 1
step "5k" 10000 2s 1 1
step "10k" 20000 2s 1 1
step "20k" 40000 2s 1 1
step "20k sampled" 40000 2s 10 1
step "20k, two taps" 40000 2s 1 2

say "Results"
echo "Cores: $(sysctl -n hw.ncpu). Every step ran ${STEP}s after ${WARM}s of warm-up."
echo
echo "| Step | Offered msg/s | MQTT read/s | MQTT missed/s | Kafka read/s | Max delay ms, Kafka / MQTT | Hop events out/s | Tap CPU (all taps) | Tap memory (largest) | Load avg (5 min) |"
echo "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"
cat /tmp/hopwake-qa-rows
