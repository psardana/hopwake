"""Normaliser: reshape raw device readings into the platform's telemetry format.

Reads JSON readings from telemetry.raw and writes telemetry.normalized:
field names change, temperature moves to kelvin and pressure to pascal.
Records that cannot be parsed go to telemetry.dlq with the reason in a header.
"""

import json
import logging
import os
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer

BOOTSTRAP = os.environ.get("NORMALISER_KAFKA", "localhost:9092")
SOURCE = os.environ.get("NORMALISER_SOURCE", "telemetry.raw")
TARGET = os.environ.get("NORMALISER_TARGET", "telemetry.normalized")
DLQ = os.environ.get("NORMALISER_DLQ", "telemetry.dlq")
GROUP = os.environ.get("NORMALISER_GROUP", "normaliser")

log = logging.getLogger("normaliser")


class Faults:
    """Failures injected on purpose, to reproduce lost and malformed data.

    FAULT_DROP drops a share of readings; FAULT_RENAME renames temp_c the way an
    unannounced firmware change would, which sends the reading to dead letters.
    FAULT_DEVICES limits both to a comma-separated list of devices.
    Each injected fault is logged as one JSON line.
    """

    def __init__(self, env=os.environ, rng=None):
        import random
        self.rng = rng or random.Random()
        self.drop = float(env.get("FAULT_DROP") or 0)
        self.rename = float(env.get("FAULT_RENAME") or 0)
        self.devices = {d.strip() for d in (env.get("FAULT_DEVICES") or "").split(",") if d.strip()}
        if self.drop or self.rename:
            log.info("faults on: drop=%.3f rename=%.3f devices=%d (0 means all)", self.drop, self.rename, len(self.devices))

    def applies(self, device: str) -> bool:
        return bool(self.drop or self.rename) and (not self.devices or device in self.devices)

    def apply(self, raw: dict):
        """Return the reading to process, changed or not, or None to drop it."""
        device = raw.get("device_id", "")
        if not self.applies(device):
            return raw
        if self.rng.random() < self.drop:
            self._log("drop", raw)
            return None
        if self.rng.random() < self.rename and "temp_c" in raw:
            self._log("rename", raw)
            raw = dict(raw)
            raw["temperature"] = raw.pop("temp_c")
        return raw

    @staticmethod
    def _log(kind: str, raw: dict) -> None:
        log.info(json.dumps({"fault": kind, "hop": "normaliser", "device": raw.get("device_id"),
                             "seq": raw.get("seq"), "at": datetime.now(timezone.utc).isoformat()}))


def normalise(raw: dict) -> dict:
    return {
        "schema": "telemetry/v2",
        "device": raw["device_id"],
        "site": raw["site"],
        "seq": int(raw["seq"]),
        "observed_at": raw["ts"],
        "temperature_k": round(float(raw["temp_c"]) + 273.15, 2),
        "pressure_pa": int(round(float(raw["pressure_kpa"]) * 1000)),
        "vibration_mm_s": float(raw["vibration_mm_s"]),
    }


def now() -> bytes:
    return datetime.now(timezone.utc).isoformat().encode()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 5})
    consumer.subscribe([SOURCE])

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    faults = Faults()
    counts = {"in": 0, "out": 0, "dlq": 0, "dropped": 0}
    last_report = time.monotonic()
    while running:
        msg = consumer.poll(0.5)
        if msg is not None:
            if msg.error():
                log.warning("consume error: %s", msg.error())
                continue
            counts["in"] += 1
            headers = list(msg.headers() or [])
            try:
                raw = faults.apply(json.loads(msg.value()))
                if raw is None:
                    counts["dropped"] += 1
                    continue
                out = normalise(raw)
            except (ValueError, KeyError, TypeError, AttributeError) as exc:
                producer.produce(DLQ, key=msg.key(), value=msg.value(),
                                 headers=headers + [("dlq_reason", str(exc).encode()), ("dlq_at", now())])
                counts["dlq"] += 1
            else:
                producer.produce(TARGET, key=msg.key(), value=json.dumps(out, separators=(",", ":")).encode(),
                                 headers=headers + [("normalised_at", now())])
                counts["out"] += 1
            producer.poll(0)
        if time.monotonic() - last_report >= 10:
            log.info("%(in)d consumed, %(out)d normalised, %(dlq)d to dead letters, %(dropped)d dropped by faults", counts)
            last_report = time.monotonic()

    producer.flush(10)
    consumer.close()
    log.info("stopped: %(in)d consumed, %(out)d normalised, %(dlq)d to dead letters, %(dropped)d dropped by faults", counts)


if __name__ == "__main__":
    main()
