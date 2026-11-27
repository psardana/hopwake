/* Tests for the device mark library: make test */
#include "hopwake_mark.h"

#include <stdio.h>
#include <string.h>

static int failures;

#define EXPECT(cond)                                                     \
    do {                                                                 \
        if (!(cond)) {                                                   \
            fprintf(stderr, "%s:%d: expected %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                  \
        }                                                                \
    } while (0)

#define EXPECT_STR(got, want)                                                      \
    do {                                                                           \
        if (strcmp((got), (want)) != 0) {                                          \
            fprintf(stderr, "%s:%d:\n  got  %s\n  want %s\n", __FILE__, __LINE__, (got), (want)); \
            failures++;                                                            \
        }                                                                          \
    } while (0)

static void one_time_mark(void) {
    hw_mark slots[4];
    hw_queue q;
    char buf[512];
    size_t n;
    hw_queue_init(&q, slots, 4);
    EXPECT(hw_mark_time(&q, "ems.sent", HW_SEND, "ems.measurements.Measurement", 1763460000750LL, 1763460000900LL) == HW_OK);
    size_t len = hw_encode(&q, buf, sizeof buf, &n);
    EXPECT(n == 1);
    EXPECT(len == strlen(buf));
    EXPECT_STR(buf, "{\"v\":1,\"marks\":[{\"hop\":\"ems.sent\",\"op\":\"send\",\"type\":\"ems.measurements.Measurement\",\"time\":1763460000750,\"at\":1763460000900}]}");
    EXPECT(hw_queue_len(&q) == 1); /* kept until committed */
    hw_commit(&q, n);
    EXPECT(hw_queue_len(&q) == 0);
    EXPECT(hw_encode(&q, buf, sizeof buf, &n) == 0 && n == 0);
}

static void seq_marks_and_escaping(void) {
    hw_mark slots[4];
    hw_queue q;
    char buf[512];
    size_t n;
    hw_queue_init(&q, slots, 4);
    hw_mark_seq(&q, "meter.sent", HW_STORE, 18446744073709551615ULL, "7", 0);
    hw_mark_seq(&q, "meter.sent", HW_SEND, 12, NULL, 5);
    hw_mark_time(&q, "x", HW_SEND, "a\"b\\c\n", -1, 0);
    hw_encode(&q, buf, sizeof buf, &n);
    EXPECT(n == 3);
    EXPECT_STR(buf, "{\"v\":1,\"marks\":["
                    "{\"hop\":\"meter.sent\",\"op\":\"store\",\"seq\":18446744073709551615,\"boot\":\"7\"},"
                    "{\"hop\":\"meter.sent\",\"op\":\"send\",\"seq\":12,\"at\":5},"
                    "{\"hop\":\"x\",\"op\":\"send\",\"type\":\"a\\\"b\\\\c\\u000a\",\"time\":-1}]}");
}

static void a_full_queue_drops_the_oldest(void) {
    hw_mark slots[3];
    hw_queue q;
    char buf[512];
    size_t n;
    hw_queue_init(&q, slots, 3);
    for (int i = 1; i <= 3; i++) {
        EXPECT(hw_mark_seq(&q, "h", HW_STORE, (uint64_t)i, "", 0) == HW_OK);
    }
    EXPECT(hw_mark_seq(&q, "h", HW_STORE, 4, "", 0) == HW_DROPPED_OLDEST);
    EXPECT(hw_mark_seq(&q, "h", HW_STORE, 5, "", 0) == HW_DROPPED_OLDEST);
    hw_encode(&q, buf, sizeof buf, &n);
    EXPECT(n == 3);
    EXPECT_STR(buf, "{\"v\":1,\"marks\":[{\"hop\":\"h\",\"op\":\"store\",\"seq\":3},{\"hop\":\"h\",\"op\":\"store\",\"seq\":4},{\"hop\":\"h\",\"op\":\"store\",\"seq\":5}],\"dropped\":2}");
    hw_commit(&q, n);
    hw_mark_seq(&q, "h", HW_SEND, 6, "", 0);
    hw_encode(&q, buf, sizeof buf, &n);
    EXPECT(strstr(buf, "dropped") == NULL); /* reported once, then cleared */
}

static void a_small_buffer_takes_what_fits(void) {
    hw_mark slots[8];
    hw_queue q;
    char buf[120];
    size_t n, total = 0, messages = 0;
    hw_queue_init(&q, slots, 8);
    for (int i = 0; i < 5; i++) {
        hw_mark_seq(&q, "ems.sent", HW_SEND, (uint64_t)(1000 + i), "", 0);
    }
    while (hw_encode(&q, buf, sizeof buf, &n) > 0) {
        EXPECT(n >= 1 && strlen(buf) < sizeof buf);
        EXPECT(buf[strlen(buf) - 1] == '}');
        total += n;
        messages++;
        hw_commit(&q, n);
    }
    EXPECT(total == 5);
    EXPECT(messages >= 2);
    /* not even one mark fits: nothing is written and nothing is lost */
    hw_mark_seq(&q, "ems.sent", HW_SEND, 1, "", 0);
    EXPECT(hw_encode(&q, buf, 20, &n) == 0 && n == 0 && buf[0] == '\0');
    EXPECT(hw_queue_len(&q) == 1);
}

static void strings_that_do_not_fit(void) {
    hw_mark slots[2];
    hw_queue q;
    char longtype[200];
    memset(longtype, 'x', sizeof longtype - 1);
    longtype[sizeof longtype - 1] = '\0';
    hw_queue_init(&q, slots, 2);
    EXPECT(hw_mark_time(&q, "ems.sent", HW_SEND, longtype, 1, 0) == HW_TOO_LONG);
    EXPECT(hw_mark_time(&q, "", HW_SEND, "t", 1, 0) == HW_TOO_LONG);
    EXPECT(hw_mark_seq(&q, NULL, HW_SEND, 1, "", 0) == HW_TOO_LONG);
    EXPECT(hw_queue_len(&q) == 0);
}

static void topics(void) {
    char t[64];
    EXPECT(hw_topic(t, sizeof t, "fleet/{device}/from/hopwake", "EMS0000042") == 29);
    EXPECT_STR(t, "fleet/EMS0000042/from/hopwake");
    EXPECT(hw_topic(t, 10, "fleet/{device}/from/hopwake", "EMS0000042") == -1);
    EXPECT(hw_topic(t, sizeof t, "no/device/here", "x") == -1);
    EXPECT(hw_topic(t, sizeof t, "{device}/{device}", "x") == -1);
}

int main(void) {
    one_time_mark();
    seq_marks_and_escaping();
    a_full_queue_drops_the_oldest();
    a_small_buffer_takes_what_fits();
    strings_that_do_not_fit();
    topics();
    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("device marks: all tests pass\n");
    return 0;
}
