package dev.hopwake.hook;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

class HookTest {
    /** A stand-in for Hopwake that keeps what it receives. */
    static final class Collector implements AutoCloseable {
        final HttpServer server;
        final List<String> bodies = new CopyOnWriteArrayList<>();

        Collector() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/v1/traces", ex -> {
                bodies.add(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                ex.sendResponseHeaders(200, -1);
                ex.close();
            });
            server.createContext("/api/aliases", ex -> {
                byte[] b = "[{\"kind\":\"systemId\",\"value\":\"100042\",\"device\":\"EMS0000042\",\"seen\":3}]".getBytes(StandardCharsets.UTF_8);
                ex.sendResponseHeaders(200, b.length);
                ex.getResponseBody().write(b);
                ex.close();
            });
            server.start();
        }

        String url() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        int spans() {
            int n = 0;
            for (String b : bodies) {
                n += b.split("\"traceId\"", -1).length - 1;
            }
            return n;
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }

    @Test
    void servesOnceStoresAlways() throws Exception {
        try (Collector c = new Collector();
             Hook h = Hook.create(HookConfig.forHop("hook:api.served").tenant("t").endpoint(c.url()))) {
            h.mark(Op.SERVE, "dev-1", 7);
            h.mark(Op.SERVE, "dev-1", 7);
            h.mark(Op.STORE, "dev-1", 7);
            h.mark(Op.STORE, "dev-1", 7);
            h.flush();
            assertEquals(3, c.spans());
            assertTrue(c.bodies.get(0).contains("\"hopwake.journey.id\",\"value\":{\"stringValue\":\"" + JourneyIds.seq("t", "dev-1", 7)));
        }
    }

    @Test
    void unknownAliasSendsNothing() throws Exception {
        try (Collector c = new Collector();
             Hook h = Hook.create(HookConfig.forHop("hook:api.served").tenant("t").endpoint(c.url()).aliases(c.url()))) {
            long deadline = System.currentTimeMillis() + 5000;
            while (h.deviceOf("systemId", "100042") == null && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertEquals("EMS0000042", h.deviceOf("systemId", "100042"));
            assertFalse(h.markTimeAlias(Op.SERVE, "systemId", "999", "x", Instant.now()));
            h.flush();
            assertEquals(0, c.spans());
            assertTrue(h.markTimeAlias(Op.SERVE, "systemId", "100042", "x", Instant.now()));
            h.flush();
            assertEquals(1, c.spans());
            assertTrue(c.bodies.get(0).contains("systemId:100042"));
        }
    }

    @Test
    void noAliasListMeansUnknown() {
        try (Hook h = Hook.create(HookConfig.forHop("hook:x").tenant("t").endpoint("http://127.0.0.1:1"))) {
            assertFalse(h.markTimeAlias(Op.SERVE, "systemId", "1", "x", Instant.now()));
        }
    }

    @Test
    void neverBlocksWhenHopwakeIsAway() {
        // Port 1 refuses connections; the queue fills and drops rather than wait.
        try (Hook h = Hook.create(HookConfig.forHop("hook:x").tenant("t").endpoint("http://127.0.0.1:1"))) {
            long start = System.nanoTime();
            for (int i = 0; i < 200_000; i++) {
                h.mark(Op.STORE, "dev", i);
            }
            long ms = (System.nanoTime() - start) / 1_000_000;
            assertTrue(ms < 3000, "200,000 marks took " + ms + " ms");
            assertTrue(h.dropped() > 0);
        }
    }

    @Test
    void tenantAndHopAreRequired() {
        assertThrows(IllegalArgumentException.class, () -> HookConfig.forHop("").tenant("t").resolve(java.util.Map.of()));
        assertThrows(IllegalArgumentException.class, () -> HookConfig.forHop("hook:x").resolve(java.util.Map.of()));
    }
}
