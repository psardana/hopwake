# Reference pipeline

A small but realistic IoT pipeline that runs on one machine. Hopwake is developed and tested against it.

Planned path of a reading:

```
devicesim ──► Mosquitto ──► bridge ──► Kafka ──► normaliser ──► aggregator ──► database ──► API ──► dashboard
```

Built so far: the device simulator, Mosquitto, the bridge, Kafka, the normaliser, the aggregator, Postgres with the recorder, the API and the portal.

```
devicesim ──► Mosquitto ──► bridge ──► Kafka ──► normaliser ──► aggregator ──► recorder ──► Postgres ──► API ──► portal
                                         telemetry.raw  telemetry.normalized  telemetry.site_minute
```

The portal is at http://localhost:8080.

## Run

```bash
docker compose -f reference/compose.yaml up -d --build
```

The fleet size and reading interval come from `DEVICES` (default 50) and `INTERVAL` (default 5s):

```bash
DEVICES=300 INTERVAL=1s docker compose -f reference/compose.yaml up -d --build
```

`fleet.txt` lists the 50 default devices, so Hopwake knows which ones should report. With another fleet size, devices beyond the list show as unexpected, and listed ones that do not run show as never seen.

Read a few records from Kafka:

```bash
docker exec hopwake-reference-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic telemetry.raw --max-messages 3 \
  --property print.key=true --property print.headers=true
```

Watch a few readings:

```bash
docker exec hopwake-reference-mosquitto-1 mosquitto_sub -t 'plant/#' -C 5 -v
```

Stop everything:

```bash
docker compose -f reference/compose.yaml down
```

## Bridge

Subscribes to `$share/bridge/plant/+/+/telemetry` and produces each payload unchanged to `telemetry.raw`, keyed by the device ID from the MQTT topic. Headers: `mqtt_topic` and `bridge_received_at`. A shared subscription lets several bridges split the load.

## Normaliser (Python)

Reads `telemetry.raw` and writes `telemetry.normalized` in the platform's own format: fields are renamed, temperature moves to kelvin and pressure to pascal. Headers are passed on, with `normalised_at` added. Records that cannot be parsed go to `telemetry.dlq` with a `dlq_reason` header.

```json
{"schema":"telemetry/v2","device":"dev-00198","site":"site-02","seq":182,"observed_at":"2026-11-18T07:03:29.33888131Z","temperature_k":299.85,"pressure_pa":100230,"vibration_mm_s":1.671}
```

## Aggregator (Java, Kafka Streams)

Groups `telemetry.normalized` by site into one-minute windows. When a window closes (10 seconds of grace), it writes the median temperature and pressure, the number of readings and the number of distinct devices to `telemetry.site_minute`, keyed by site. This is the many-to-one hop: one output stands for thousands of readings from dozens of devices.

```json
{"site":"site-00","window_start":"2026-11-18T07:10:00Z","window_end":"2026-11-18T07:11:00Z","readings":4516,"devices":75,"median_temperature_k":297.41,"median_pressure_pa":101320}
```

Unit tests run during the image build (`mvn package`).

## Recorder (Go) and Postgres

Consumes `telemetry.normalized` and `telemetry.site_minute` and writes them to Postgres (`readings` and `site_minute`, see `postgres/init.sql`) in batches. A reading that arrives twice is stored once (primary key: device and sequence number). Offsets are committed only after a batch is written. Readings older than 24 hours are pruned.

## API (TypeScript) and portal

The API serves stored data, and the portal page at `/` is the end of the journey: what an operator sees.

| Endpoint | Returns |
| --- | --- |
| `GET /api/sites` | The last closed minute of every site |
| `GET /api/sites/{site}/minutes?limit=60` | Recent minutes of one site |
| `GET /api/sites/{site}/devices` | Devices of a site with when each was last seen |
| `GET /api/devices/{device}/readings?limit=50` | Latest readings of one device |
| `GET /healthz` | Database reachable |

## Faults

Faults reproduce the tickets Hopwake is meant to answer: data that never arrives, arrives late, arrives twice, or arrives in a shape the next hop rejects. They are off by default and set through environment variables when the stack starts. Every injected fault is logged as one JSON line with hop, device and sequence number, so there is a record of what really happened to check a tracer against.

| Variable | Hop | Effect |
| --- | --- | --- |
| `BRIDGE_FAULT_DEVICES` | bridge | Comma-separated devices the bridge faults apply to (empty: all) |
| `BRIDGE_FAULT_DROP` | bridge | Share of readings dropped, 0 to 1 |
| `BRIDGE_FAULT_DUPLICATE` | bridge | Share of readings sent twice |
| `BRIDGE_FAULT_DELAY`, `BRIDGE_FAULT_DELAY_RATE` | bridge | Hold a share of readings back for this long, which also reorders them |
| `NORMALISER_FAULT_DEVICES` | normaliser | Devices the normaliser faults apply to (empty: all) |
| `NORMALISER_FAULT_DROP` | normaliser | Share of readings dropped |
| `NORMALISER_FAULT_RENAME` | normaliser | Share of readings with `temp_c` renamed, as an unannounced firmware change would; they end up in `telemetry.dlq` |

Example: one device silently lost at the bridge, another broken at the normaliser.

```bash
BRIDGE_FAULT_DEVICES=dev-00042 BRIDGE_FAULT_DROP=1 \
NORMALISER_FAULT_DEVICES=dev-00043 NORMALISER_FAULT_RENAME=1 \
docker compose -f reference/compose.yaml up -d bridge normaliser
```

In the portal both devices stop updating while the rest of their site carries on. The fault log:

```bash
docker compose -f reference/compose.yaml logs bridge normaliser | grep '"fault"'
```

## Tracing with OpenTelemetry

Every service is instrumented with stock OpenTelemetry (Go and Python SDKs, the Java agent, Node auto-instrumentation). It is off by default; `tracing.env` turns it on and starts Jaeger:

```bash
docker compose -f reference/compose.yaml --env-file reference/tracing.env up -d --build
```

Jaeger: http://localhost:16686. `OTEL_SAMPLE` sets the sampling ratio. What these traces can and cannot answer is written up in [docs/baseline-opentelemetry.md](../docs/baseline-opentelemetry.md).

## Device simulator

Each device publishes JSON to `plant/<site>/<device>/telemetry` with a per-device sequence number:

```json
{"device_id":"dev-00038","site":"site-02","seq":1,"ts":"2026-11-18T06:38:18.434825Z","temp_c":24.16,"pressure_kpa":101.47,"vibration_mm_s":0.999}
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `-broker` | `tcp://localhost:1883` | MQTT broker |
| `-devices` | 1000 | Devices to simulate (Compose passes `DEVICES`, default 50) |
| `-sites` | 4 | Sites the devices are spread over |
| `-interval` | 5s | Time between readings per device |
| `-conns` | 10 | MQTT connections shared by the devices |
| `-qos` | 1 | QoS 0 or 1 |
| `-duration` | 0 | Stop after this long; 0 runs until interrupted |
| `-rename-devices` | none | Devices whose firmware sends `temperature` instead of `temp_c` (Compose: `FIRMWARE_RENAME_DEVICES`) |
| `-rename-after` | 0s | When that firmware change takes effect, after start (Compose: `FIRMWARE_RENAME_AFTER`) |
