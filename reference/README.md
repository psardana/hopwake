# Reference pipeline

A small but realistic IoT pipeline that runs on one machine. Hopwake is developed and tested against it.

Planned path of a reading:

```
devicesim ──► Mosquitto ──► bridge ──► Kafka ──► normaliser ──► aggregator ──► database ──► API ──► dashboard
```

Built so far: the device simulator, Mosquitto, the bridge, Kafka, the normaliser and the aggregator.

## Run

```bash
docker compose -f reference/compose.yaml up -d --build
```

The fleet size and reading interval come from `DEVICES` (default 1000) and `INTERVAL` (default 5s):

```bash
DEVICES=300 INTERVAL=1s docker compose -f reference/compose.yaml up -d --build
```

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

## Device simulator

Each device publishes JSON to `plant/<site>/<device>/telemetry` with a per-device sequence number:

```json
{"device_id":"dev-00038","site":"site-02","seq":1,"ts":"2026-11-18T06:38:18.434825Z","temp_c":24.16,"pressure_kpa":101.47,"vibration_mm_s":0.999}
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `-broker` | `tcp://localhost:1883` | MQTT broker |
| `-devices` | 1000 | Devices to simulate |
| `-sites` | 4 | Sites the devices are spread over |
| `-interval` | 5s | Time between readings per device |
| `-conns` | 10 | MQTT connections shared by the devices |
| `-qos` | 1 | QoS 0 or 1 |
| `-duration` | 0 | Stop after this long; 0 runs until interrupted |
