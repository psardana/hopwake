package dev.hopwake.hook.spring;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import dev.hopwake.hook.JourneyIds;
import dev.hopwake.hook.Op;
import dev.hopwake.hook.Reading;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * A Spring Boot service with a batch @KafkaListener and a KafkaTemplate, as an energy-storage platform's measurement
 * service has: the starter marks what the listener handled and what the template sent, and keeps
 * the interceptor the application set itself.
 */
@SpringBootTest(classes = KafkaMarksTest.App.class, properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.consumer.group-id=svc",
        "spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
        "spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
        "spring.kafka.listener.type=batch",
        "spring.application.name=measurement-service",
        "hopwake.tenant=t",
        "hopwake.time-precision=1s",
        "hopwake.kafka.time-header=MeasurementTime",
        "hopwake.kafka.type-header=MessageType",
        "hopwake.kafka.marks[0].topic=in",
        "hopwake.kafka.marks[0].hop=hook:svc.received",
        "hopwake.kafka.marks[1].topic=out",
        "hopwake.kafka.marks[1].hop=hook:svc.sent",
        "hopwake.kafka.marks[1].on=send",
})
@EmbeddedKafka(partitions = 1, topics = {"in", "out"})
class KafkaMarksTest {
    static final List<String> bodies = new CopyOnWriteArrayList<>();
    static final CountDownLatch handled = new CountDownLatch(2);
    static final CountDownLatch ownInterceptor = new CountDownLatch(2);
    static HttpServer collector;

    @DynamicPropertySource
    static void collector(DynamicPropertyRegistry r) throws Exception {
        collector = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        collector.createContext("/v1/traces", ex -> {
            bodies.add(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            ex.sendResponseHeaders(200, -1);
            ex.close();
        });
        collector.start();
        r.add("hopwake.endpoint", () -> "http://127.0.0.1:" + collector.getAddress().getPort());
    }

    @AfterAll
    static void stop() {
        collector.stop(0);
    }

    @SpringBootApplication
    static class App {
        @KafkaListener(topics = "in", batch = "true")
        void consume(List<ConsumerRecord<String, String>> records) {
            records.forEach(r -> handled.countDown());
        }

        // An interceptor of the application's own, which the starter must keep.
        @Bean
        RecordInterceptor<Object, Object> ownRecordInterceptor() {
            return (record, consumer) -> record;
        }

        @Bean
        org.springframework.kafka.listener.BatchInterceptor<Object, Object> ownBatchInterceptor() {
            return (records, consumer) -> {
                records.forEach(r -> ownInterceptor.countDown());
                return records;
            };
        }
    }

    @Autowired
    KafkaTemplate<String, String> template;
    @Autowired
    Hooks hooks;

    @Test
    void marksWhatTheServiceHandledAndSent() throws Exception {
        Instant at = Instant.parse("2026-11-18T10:00:00.750Z");
        for (String serial : List.of("EMS0000001", "EMS0000002")) {
            ProducerRecord<String, String> r = new ProducerRecord<>("in", serial, "payload");
            r.headers().add("MeasurementTime", at.toString().getBytes(StandardCharsets.UTF_8));
            r.headers().add("MessageType", "ems.measurements.Measurement".getBytes(StandardCharsets.UTF_8));
            template.send(r).get(10, TimeUnit.SECONDS);
        }
        assertThat(handled.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(ownInterceptor.await(5, TimeUnit.SECONDS)).as("the application's own batch interceptor still runs").isTrue();

        ProducerRecord<String, String> out = new ProducerRecord<>("out", "EMS0000001", "x");
        out.headers().add("MeasurementTime", at.toString().getBytes(StandardCharsets.UTF_8));
        out.headers().add("MessageType", "ems.measurements.Measurement".getBytes(StandardCharsets.UTF_8));
        template.send(out).get(10, TimeUnit.SECONDS);

        // Sends to "in" are not marked (only "out" is a send mark); consumes of "in" are.
        String id1 = JourneyIds.time("t", "EMS0000001", "ems.measurements.Measurement", "2026-11-18T10:00:00Z");
        String id2 = JourneyIds.time("t", "EMS0000002", "ems.measurements.Measurement", "2026-11-18T10:00:00Z");
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline && count("\"hopwake.op\",\"value\":{\"stringValue\":\"route\"") < 1) {
            Thread.sleep(100);
        }
        String all = String.join("\n", bodies);
        assertThat(count("\"hopwake.op\",\"value\":{\"stringValue\":\"receive\"")).isEqualTo(2);
        assertThat(count("\"hopwake.op\",\"value\":{\"stringValue\":\"route\"")).isEqualTo(1);
        assertThat(all).contains(id1).contains(id2).contains("hook:svc.received").contains("hook:svc.sent")
                .contains("\"service.name\",\"value\":{\"stringValue\":\"measurement-service\"");
    }

    @Test
    void servicesMarkStoresAndServesThemselves() throws Exception {
        assertThat(hooks.mark("hook:lvc.stored", Op.STORE, Reading.time("EMS0000003", "m", Instant.now()).teach("systemId", "100003"))).isTrue();
        assertThat(hooks.mark("hook:api.served", Op.SERVE, Reading.byAlias("systemId", "100003", "m", Instant.now())))
                .as("no alias list configured: unknown").isFalse();
        hooks.hook("hook:lvc.stored").flush();
        assertThat(String.join("\n", bodies)).contains("systemId:100003");
    }

    private static int count(String needle) {
        int n = 0;
        for (String b : bodies) {
            n += b.split(java.util.regex.Pattern.quote(needle), -1).length - 1;
        }
        return n;
    }

    static Duration unused() {
        return Duration.ZERO;
    }
}
