package dev.hopwake.hook;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lets a service say that it stored or served a reading, for hops no tap can watch from outside,
 * such as a database write or an HTTP response.
 *
 * <pre>
 * Hook stored = Hook.create(HookConfig.forHop("hook:lvc.stored").tenant("fleet"));
 * stored.markTime(Op.STORE, serial, "ems.measurements.Measurement", measurementTime);
 * </pre>
 *
 * <p>Marks are hop events, sent as OTLP/JSON spans with the journey ID a tap derives for the same
 * reading. They are queued and sent once a second from a background thread; when Hopwake cannot be
 * reached they are dropped. A mark never blocks or throws. A reading is marked served only the
 * first time. One hook is safe to use from many threads.
 */
public final class Hook implements AutoCloseable {
    private static final int SERVED_LIMIT = 100_000;

    private final HookConfig cfg;
    private final HttpClient http;
    private final URI traces;
    private final BlockingQueue<Mark> queue;
    private final ScheduledExecutorService timer;
    private final AliasList aliases;
    private final Set<String> served = new HashSet<>();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong sent = new AtomicLong();
    private final String resource;

    private Hook(HookConfig cfg) {
        this.cfg = cfg;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
        this.traces = URI.create(cfg.endpoint + "/v1/traces");
        this.queue = new ArrayBlockingQueue<>(cfg.queueSize);
        this.timer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "hopwake-hook " + cfg.hop);
            t.setDaemon(true);
            return t;
        });
        StringBuilder b = new StringBuilder("{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":");
        Json.string(b, cfg.service);
        b.append("}}]},\"scopeSpans\":[{\"scope\":{\"name\":\"hopwake/hook\"},\"spans\":[");
        this.resource = b.toString();
        this.aliases = cfg.aliases == null ? null : new AliasList(cfg.aliases, http, timer);
        timer.scheduleWithFixedDelay(this::flushQuietly, 1, 1, TimeUnit.SECONDS);
    }

    /** Starts a hook. It does not contact Hopwake until there are marks to send. */
    public static Hook create(HookConfig config) {
        return new Hook(config.resolve(System.getenv()));
    }

    /** The hop this hook marks. */
    public String hop() {
        return cfg.hop;
    }

    /** The tenant this hook marks for. */
    public String tenant() {
        return cfg.tenant;
    }

    /** Marks one reading with a sequence number, now. */
    public void mark(Op op, String device, long seq) {
        markBoot(op, device, "", seq);
    }

    /** {@link #mark} for sources that name a boot field. */
    public void markBoot(Op op, String device, String boot, long seq) {
        String id = JourneyIds.seq(cfg.tenant, device, boot, seq);
        enqueue(op, id, device, seq, null, null);
    }

    /**
     * Marks a reading that carries no sequence number. It is known by its device, message type and
     * the time the device gave it, exactly as a tap derives its journey ID.
     */
    public void markTime(Op op, String device, String messageType, Instant at) {
        enqueue(op, timeId(device, messageType, at), device, 0, messageType, null);
    }

    /**
     * {@link #markTime} at a hop that knows both the device and another name it goes by further
     * down, such as the system ID a store keys it by. Hopwake learns the pair from this mark.
     */
    public void markTimeTeach(Op op, String device, String aliasKind, String aliasValue, String messageType, Instant at) {
        enqueue(op, timeId(device, messageType, at), device, 0, messageType, Aliases.format(aliasKind, aliasValue));
    }

    /**
     * {@link #markTime} for a hop that knows the device only by an alias. Returns false, and sends
     * nothing, while Hopwake has not learned which device the alias stands for.
     */
    public boolean markTimeAlias(Op op, String aliasKind, String aliasValue, String messageType, Instant at) {
        String device = aliases == null ? null : aliases.device(aliasKind, aliasValue);
        if (device == null) {
            return false;
        }
        enqueue(op, timeId(device, messageType, at), device, 0, messageType, Aliases.format(aliasKind, aliasValue));
        return true;
    }

    /**
     * Marks one reading, however it is known. Returns false, and sends nothing, only for a reading
     * known by an alias Hopwake has not learned yet.
     */
    public boolean mark(Op op, Reading r) {
        switch (r.kind) {
            case SEQ:
                markBoot(op, r.device, r.boot, r.seq);
                return true;
            case TIME:
                markTime(op, r.device, r.type, r.time);
                return true;
            case TEACH:
                markTimeTeach(op, r.device, r.aliasKind, r.aliasValue, r.type, r.time);
                return true;
            default:
                return markTimeAlias(op, r.aliasKind, r.aliasValue, r.type, r.time);
        }
    }

    /** The device an alias stands for, or null while it is not known. */
    public String deviceOf(String aliasKind, String aliasValue) {
        return aliases == null ? null : aliases.device(aliasKind, aliasValue);
    }

    /** Marks dropped because the queue was full or Hopwake could not be reached. */
    public long dropped() {
        return dropped.get();
    }

    /** Marks Hopwake accepted. */
    public long sent() {
        return sent.get();
    }

    private String timeId(String device, String messageType, Instant at) {
        return JourneyIds.time(cfg.tenant, device, messageType, HopTimes.format(at, cfg.timePrecision));
    }

    private void enqueue(Op op, String id, String device, long seq, String messageType, String alias) {
        if (op == Op.SERVE) {
            synchronized (served) {
                if (!served.add(id)) {
                    return;
                }
                if (served.size() > SERVED_LIMIT) {
                    served.clear();
                    served.add(id);
                }
            }
        }
        Instant now = Instant.now();
        // Only the facts are queued; the sender thread writes the JSON.
        if (!queue.offer(new Mark(op, id, device, seq, messageType, alias, now.getEpochSecond() * 1_000_000_000L + now.getNano()))) {
            dropped.incrementAndGet();
        }
    }

    private static final class Mark {
        final Op op;
        final String id, device, messageType, alias;
        final long seq, unixNanos;

        Mark(Op op, String id, String device, long seq, String messageType, String alias, long unixNanos) {
            this.op = op;
            this.id = id;
            this.device = device;
            this.seq = seq;
            this.messageType = messageType;
            this.alias = alias;
            this.unixNanos = unixNanos;
        }
    }

    private void span(StringBuilder b, Mark m) {
        Op op = m.op;
        String id = m.id, device = m.device, messageType = m.messageType, alias = m.alias;
        long seq = m.seq;
        String ns = Long.toString(m.unixNanos);
        byte[] spanId = new byte[8];
        do {
            ThreadLocalRandom.current().nextBytes(spanId);
        } while (allZero(spanId));
        b.append("{\"traceId\":\"").append(id).append("\",\"spanId\":\"").append(Hex.of(spanId)).append("\",\"name\":");
        Json.string(b, "hop " + cfg.hop);
        b.append(",\"kind\":5,\"startTimeUnixNano\":\"").append(ns).append("\",\"endTimeUnixNano\":\"").append(ns).append("\",\"attributes\":[");
        attr(b, "hopwake.schema", "hop/v1").append(',');
        attr(b, "hopwake.tenant", cfg.tenant).append(',');
        attr(b, "hopwake.device.id", device).append(',');
        b.append("{\"key\":\"hopwake.seq\",\"value\":{\"intValue\":\"").append(Long.toUnsignedString(seq)).append("\"}},");
        attr(b, "hopwake.journey.id", id).append(',');
        attr(b, "hopwake.hop", cfg.hop).append(',');
        attr(b, "hopwake.op", op.wire()).append(',');
        attr(b, "hopwake.correlation", "derived").append(',');
        b.append("{\"key\":\"hopwake.payload.bytes\",\"value\":{\"intValue\":\"0\"}},");
        attr(b, "messaging.system", "hook");
        if (messageType != null && !messageType.isEmpty()) {
            attr(b.append(','), "hopwake.message.type", messageType);
        }
        if (alias != null) {
            attr(b.append(','), "hopwake.device.alias", alias);
        }
        b.append("]}");
    }

    private static StringBuilder attr(StringBuilder b, String key, String value) {
        b.append("{\"key\":\"").append(key).append("\",\"value\":{\"stringValue\":");
        Json.string(b, value == null ? "" : value);
        return b.append("}}");
    }

    private static boolean allZero(byte[] b) {
        for (byte x : b) {
            if (x != 0) {
                return false;
            }
        }
        return true;
    }

    private void flushQuietly() {
        try {
            flush();
        } catch (RuntimeException e) {
            // A hook never takes the service down.
        }
    }

    /** Sends what is queued, waiting at most about 3 seconds per batch. */
    public void flush() {
        List<Mark> batch = new ArrayList<>();
        while (queue.drainTo(batch, 4096) > 0) {
            StringBuilder body = new StringBuilder(resource.length() + batch.size() * 640);
            body.append(resource);
            for (int i = 0; i < batch.size(); i++) {
                if (i > 0) {
                    body.append(',');
                }
                span(body, batch.get(i));
            }
            body.append("]}]}]}");
            try {
                HttpRequest req = HttpRequest.newBuilder(traces)
                        .timeout(Duration.ofSeconds(3))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                        .build();
                HttpResponse<Void> resp = http.send(req, HttpResponse.BodyHandlers.discarding());
                if (resp.statusCode() / 100 == 2) {
                    sent.addAndGet(batch.size());
                } else {
                    dropped.addAndGet(batch.size());
                }
            } catch (InterruptedException e) {
                dropped.addAndGet(batch.size());
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                // Hopwake is not reachable: these marks are lost, the service carries on.
                dropped.addAndGet(batch.size());
            }
            batch.clear();
        }
    }

    /** Sends what is queued and stops the hook. */
    @Override
    public void close() {
        timer.shutdown();
        try {
            timer.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        flushQuietly();
    }
}
