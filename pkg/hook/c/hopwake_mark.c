#include "hopwake_mark.h"

#include <stdio.h>
#include <string.h>

void hw_queue_init(hw_queue *q, hw_mark *slots, size_t cap) {
    q->slots = slots;
    q->cap = cap;
    q->head = 0;
    q->len = 0;
    q->dropped = 0;
}

static int copy(char *dst, size_t cap, const char *src) {
    size_t n = src ? strlen(src) : 0;
    if (n >= cap) {
        return -1;
    }
    if (n) {
        memcpy(dst, src, n);
    }
    dst[n] = '\0';
    return 0;
}

/* The slot for a new mark, dropping the oldest when the queue is full. */
static hw_mark *slot(hw_queue *q, int *result) {
    *result = HW_OK;
    if (q->cap == 0) {
        return NULL;
    }
    if (q->len == q->cap) {
        q->head = (q->head + 1) % q->cap;
        q->len--;
        q->dropped++;
        *result = HW_DROPPED_OLDEST;
    }
    return &q->slots[(q->head + q->len) % q->cap];
}

int hw_mark_time(hw_queue *q, const char *hop, hw_op op, const char *type, int64_t time_ms, int64_t at_ms) {
    hw_mark m;
    if (copy(m.hop, sizeof m.hop, hop) || !hop || !*hop || copy(m.type, sizeof m.type, type)) {
        return HW_TOO_LONG;
    }
    m.boot[0] = '\0';
    m.seq = 0;
    m.has_seq = 0;
    m.time_ms = time_ms;
    m.at_ms = at_ms;
    m.op = (uint8_t)op;
    int r;
    hw_mark *s = slot(q, &r);
    if (!s) {
        q->dropped++;
        return HW_DROPPED_OLDEST;
    }
    *s = m;
    q->len++;
    return r;
}

int hw_mark_seq(hw_queue *q, const char *hop, hw_op op, uint64_t seq, const char *boot, int64_t at_ms) {
    hw_mark m;
    if (copy(m.hop, sizeof m.hop, hop) || !hop || !*hop || copy(m.boot, sizeof m.boot, boot)) {
        return HW_TOO_LONG;
    }
    m.type[0] = '\0';
    m.seq = seq;
    m.has_seq = 1;
    m.time_ms = 0;
    m.at_ms = at_ms;
    m.op = (uint8_t)op;
    int r;
    hw_mark *s = slot(q, &r);
    if (!s) {
        q->dropped++;
        return HW_DROPPED_OLDEST;
    }
    *s = m;
    q->len++;
    return r;
}

size_t hw_queue_len(const hw_queue *q) { return q->len; }

/* A small writer that stops at the end of the buffer and remembers it did. */
typedef struct {
    char *buf;
    size_t cap;
    size_t len;
    int full;
} out;

static void put(out *o, const char *s, size_t n) {
    if (o->full || o->len + n >= o->cap) {
        o->full = 1;
        return;
    }
    memcpy(o->buf + o->len, s, n);
    o->len += n;
}

static void puts_(out *o, const char *s) { put(o, s, strlen(s)); }

static void put_string(out *o, const char *s) {
    put(o, "\"", 1);
    for (; *s; s++) {
        unsigned char c = (unsigned char)*s;
        char e[8];
        if (c == '"' || c == '\\') {
            e[0] = '\\';
            e[1] = (char)c;
            put(o, e, 2);
        } else if (c < 0x20) {
            snprintf(e, sizeof e, "\\u%04x", c);
            put(o, e, 6);
        } else {
            put(o, (const char *)s, 1);
        }
    }
    put(o, "\"", 1);
}

static void put_i64(out *o, int64_t v) {
    char b[24];
    int n = snprintf(b, sizeof b, "%lld", (long long)v);
    put(o, b, (size_t)n);
}

static void put_u64(out *o, uint64_t v) {
    char b[24];
    int n = snprintf(b, sizeof b, "%llu", (unsigned long long)v);
    put(o, b, (size_t)n);
}

static void put_mark(out *o, const hw_mark *m) {
    puts_(o, "{\"hop\":");
    put_string(o, m->hop);
    puts_(o, m->op == HW_STORE ? ",\"op\":\"store\"" : ",\"op\":\"send\"");
    if (m->has_seq) {
        puts_(o, ",\"seq\":");
        put_u64(o, m->seq);
        if (m->boot[0]) {
            puts_(o, ",\"boot\":");
            put_string(o, m->boot);
        }
    } else {
        if (m->type[0]) {
            puts_(o, ",\"type\":");
            put_string(o, m->type);
        }
        puts_(o, ",\"time\":");
        put_i64(o, m->time_ms);
    }
    if (m->at_ms) {
        puts_(o, ",\"at\":");
        put_i64(o, m->at_ms);
    }
    puts_(o, "}");
}

size_t hw_encode(const hw_queue *q, char *buf, size_t cap, size_t *count) {
    *count = 0;
    if (q->len == 0 || cap == 0) {
        return 0;
    }
    out o = {buf, cap, 0, 0};
    puts_(&o, "{\"v\":1,\"marks\":[");
    size_t done = 0;
    for (size_t i = 0; i < q->len; i++) {
        size_t before = o.len;
        if (i > 0) {
            puts_(&o, ",");
        }
        put_mark(&o, &q->slots[(q->head + i) % q->cap]);
        /* room for the closing "]" plus the dropped count */
        if (o.full || o.len + 32 >= cap) {
            o.len = before;
            o.full = 0;
            break;
        }
        done++;
    }
    if (done == 0) {
        buf[0] = '\0';
        return 0;
    }
    puts_(&o, "]");
    if (q->dropped) {
        puts_(&o, ",\"dropped\":");
        put_u64(&o, q->dropped);
    }
    puts_(&o, "}");
    if (o.full) {
        buf[0] = '\0';
        return 0;
    }
    buf[o.len] = '\0';
    *count = done;
    return o.len;
}

void hw_commit(hw_queue *q, size_t count) {
    if (count > q->len) {
        count = q->len;
    }
    q->head = q->cap ? (q->head + count) % q->cap : 0;
    q->len -= count;
    q->dropped = 0;
}

int hw_topic(char *outbuf, size_t cap, const char *pattern, const char *device) {
    const char *at = strstr(pattern, "{device}");
    if (!at || strstr(at + 8, "{device}")) {
        return -1;
    }
    size_t a = (size_t)(at - pattern), d = strlen(device), b = strlen(at + 8);
    if (a + d + b >= cap) {
        return -1;
    }
    memcpy(outbuf, pattern, a);
    memcpy(outbuf + a, device, d);
    memcpy(outbuf + a + d, at + 8, b);
    outbuf[a + d + b] = '\0';
    return (int)(a + d + b);
}
