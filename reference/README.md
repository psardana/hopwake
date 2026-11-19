# Reference pipeline

A small but realistic IoT pipeline that runs on one machine. Hopwake is developed and tested against it.

Planned path of a reading:

```
devicesim ──► Mosquitto ──► bridge ──► Kafka ──► normaliser ──► aggregator ──► database ──► API ──► dashboard
```

Built so far: the device simulator, Mosquitto and Kafka.

## Run

```bash
docker compose -f reference/compose.yaml up -d
go run ./reference/devicesim -devices 1000 -interval 5s
```

Watch a few readings:

```bash
docker exec hopwake-reference-mosquitto-1 mosquitto_sub -t 'plant/#' -C 5 -v
```

Stop everything:

```bash
docker compose -f reference/compose.yaml down
```

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
