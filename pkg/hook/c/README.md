# Hopwake device marks in C

A device marks its own sends with a small message on its own MQTT topic. The tap turns it into
a hop event and derives the journey ID itself, so the device computes no hashes and needs no
HTTP client. The message format and the rules for it are in
[docs/rules.md](../../../docs/rules.md#device-marks).

Plain C99 (also usable from C++), two files, no heap: you give the queue its slots and the
encoder its buffer. It is not thread-safe; use one queue per task, or guard it.

```c
#include "hopwake_mark.h"

static hw_mark slots[32];        /* about 160 bytes each */
static hw_queue marks;
static char topic[64];

void setup(void) {
    hw_queue_init(&marks, slots, 32);
    hw_topic(topic, sizeof topic, "fleet/{device}/from/hopwake", serial);
}

/* where the firmware publishes a reading */
void on_measurement_published(int64_t measured_ms) {
    hw_mark_time(&marks, "ems.sent", HW_SEND, "ems.measurements.Measurement", measured_ms, now_ms());
}

/* whenever the link is up, e.g. once a second */
void flush_marks(void) {
    char buf[1024];
    size_t n;
    size_t len = hw_encode(&marks, buf, sizeof buf, &n);
    if (len > 0 && mqtt_publish(topic, buf, len, 1 /* QoS */) == 0) {
        hw_commit(&marks, n);    /* only now are they forgotten */
    }
}
```

- `hw_mark_time` is for readings known by message type and their own time; `hw_mark_seq` for
  readings with a sequence number.
- Use `HW_STORE` to say a reading was kept while offline, then `HW_SEND` when it went out.
- When the queue is full, the oldest mark is dropped; the next message says how many.
- `hw_encode` writes as many marks as fit in the buffer. They stay queued until `hw_commit`, so
  a publish that fails loses nothing.

## Testing

```bash
make test                                   # the library's own tests
go test ./internal/devmark -run FromC       # messages written by C, read by the tap
```
