/*
 * Hopwake device marks: a device says it sent (or kept) a reading, in a small
 * message on its own MQTT topic. The tap derives the journey ID, so the device
 * computes no hashes and needs no HTTP client.
 *
 * Plain C99, no heap: the caller gives the queue its slots and the encoder its
 * buffer. Not thread-safe; use one queue per task or guard it.
 *
 *   static hw_mark slots[32];
 *   static hw_queue q;
 *   hw_queue_init(&q, slots, 32);
 *
 *   // where the firmware publishes a reading:
 *   hw_mark_time(&q, "ems.sent", HW_SEND, "ems.measurements.Measurement", measured_ms, now_ms);
 *
 *   // whenever the link is up:
 *   size_t n;
 *   size_t len = hw_encode(&q, buf, sizeof buf, &n);
 *   if (len > 0 && mqtt_publish(topic, buf, len, QOS1) == OK) hw_commit(&q, n);
 *
 * See docs/hooks.md ("Device marks") and docs/rules.md for the rules side.
 */
#ifndef HOPWAKE_MARK_H
#define HOPWAKE_MARK_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define HW_MARK_VERSION 1
#define HW_HOP_MAX 32   /* including the terminating NUL */
#define HW_TYPE_MAX 80
#define HW_BOOT_MAX 24

typedef enum {
    HW_SEND = 0,  /* the device published the reading */
    HW_STORE = 1  /* the device kept the reading, e.g. while offline */
} hw_op;

/* One kept mark. Treat as opaque. */
typedef struct {
    char hop[HW_HOP_MAX];
    char type[HW_TYPE_MAX];
    char boot[HW_BOOT_MAX];
    uint64_t seq;
    int64_t time_ms;
    int64_t at_ms;
    uint8_t op;
    uint8_t has_seq;
} hw_mark;

/* Marks waiting to be published. When full, the oldest mark is dropped and counted. */
typedef struct {
    hw_mark *slots;
    size_t cap;
    size_t head;
    size_t len;
    uint32_t dropped;
} hw_queue;

/* Results of hw_mark_*. */
#define HW_OK 0
#define HW_DROPPED_OLDEST 1 /* kept, but the queue was full and the oldest mark went */
#define HW_TOO_LONG (-1)    /* a string does not fit; nothing kept */

void hw_queue_init(hw_queue *q, hw_mark *slots, size_t cap);

/* A reading known by its message type and its own time (Unix ms). at_ms is when
 * the device did it (Unix ms), or 0 when the device has no clock it trusts. */
int hw_mark_time(hw_queue *q, const char *hop, hw_op op, const char *type, int64_t time_ms, int64_t at_ms);

/* A reading with a sequence number; boot may be NULL or "". */
int hw_mark_seq(hw_queue *q, const char *hop, hw_op op, uint64_t seq, const char *boot, int64_t at_ms);

/* Marks waiting. */
size_t hw_queue_len(const hw_queue *q);

/* Writes as many waiting marks as fit into buf as one message, oldest first,
 * NUL-terminated. Returns its length (without the NUL), or 0 when nothing is
 * waiting or not even one mark fits. *count says how many marks it holds. The
 * marks stay queued until hw_commit, so a failed publish loses nothing. */
size_t hw_encode(const hw_queue *q, char *buf, size_t cap, size_t *count);

/* Forgets the first count marks, after the message holding them was published.
 * Also clears the dropped count, which that message reported. */
void hw_commit(hw_queue *q, size_t count);

/* Writes the device's mark topic from a pattern with one "{device}", e.g.
 * "fleet/{device}/from/hopwake". Returns its length, or -1 when it does not fit. */
int hw_topic(char *out, size_t cap, const char *pattern, const char *device);

#ifdef __cplusplus
}
#endif

#endif
