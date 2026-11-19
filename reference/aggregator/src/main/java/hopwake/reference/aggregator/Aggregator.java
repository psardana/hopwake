package hopwake.reference.aggregator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;

/**
 * Per-site, per-minute aggregates of normalised telemetry.
 *
 * <p>Reads telemetry.normalized, groups readings by site into one-minute windows, and when a
 * window closes writes the median temperature and pressure, the number of readings and the number
 * of distinct devices to telemetry.site_minute, keyed by site.
 */
public final class Aggregator {
  static final ObjectMapper JSON = new ObjectMapper();

  /** Window state: every reading's values and the devices that sent them. */
  public static final class Window {
    public List<Double> temperaturesK = new ArrayList<>();
    public List<Double> pressuresPa = new ArrayList<>();
    public TreeSet<String> devices = new TreeSet<>();
  }

  private Aggregator() {}

  static Topology topology(String source, String target, Duration size, Duration grace) {
    StreamsBuilder builder = new StreamsBuilder();
    builder.stream(source, Consumed.with(Serdes.String(), Serdes.String()))
        .flatMap((device, value) -> {
          try {
            JsonNode n = JSON.readTree(value);
            return List.of(KeyValue.pair(n.get("site").asText(), n));
          } catch (Exception e) {
            return List.of(); // unreadable records are the normaliser's dead letters, not ours
          }
        })
        .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(JsonNode.class)))
        .windowedBy(TimeWindows.ofSizeAndGrace(size, grace))
        .aggregate(
            Window::new,
            (site, reading, w) -> {
              w.temperaturesK.add(reading.get("temperature_k").asDouble());
              w.pressuresPa.add(reading.get("pressure_pa").asDouble());
              w.devices.add(reading.get("device").asText());
              return w;
            },
            Materialized.with(Serdes.String(), new JsonSerde<>(Window.class)))
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .map((windowed, w) -> {
          ObjectNode out = JSON.createObjectNode();
          out.put("site", windowed.key());
          out.put("window_start", Instant.ofEpochMilli(windowed.window().start()).toString());
          out.put("window_end", Instant.ofEpochMilli(windowed.window().end()).toString());
          out.put("readings", w.temperaturesK.size());
          out.put("devices", w.devices.size());
          out.put("median_temperature_k", round(median(w.temperaturesK), 2));
          out.put("median_pressure_pa", Math.round(median(w.pressuresPa)));
          return KeyValue.pair(windowed.key(), out.toString());
        })
        .to(target, Produced.with(Serdes.String(), Serdes.String()));
    return builder.build();
  }

  static double median(List<Double> values) {
    if (values.isEmpty()) {
      return Double.NaN;
    }
    List<Double> sorted = new ArrayList<>(values);
    Collections.sort(sorted);
    int mid = sorted.size() / 2;
    return sorted.size() % 2 == 1 ? sorted.get(mid) : (sorted.get(mid - 1) + sorted.get(mid)) / 2;
  }

  static double round(double v, int places) {
    double p = Math.pow(10, places);
    return Math.round(v * p) / p;
  }

  private static String env(String key, String fallback) {
    String v = System.getenv(key);
    return v == null || v.isBlank() ? fallback : v;
  }

  public static void main(String[] args) throws InterruptedException {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, env("AGGREGATOR_APP_ID", "aggregator"));
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env("AGGREGATOR_KAFKA", "localhost:9092"));
    props.put(StreamsConfig.STATE_DIR_CONFIG, env("AGGREGATOR_STATE_DIR", "/tmp/aggregator"));
    props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);

    Topology topology = topology(
        env("AGGREGATOR_SOURCE", "telemetry.normalized"),
        env("AGGREGATOR_TARGET", "telemetry.site_minute"),
        Duration.ofSeconds(Long.parseLong(env("AGGREGATOR_WINDOW_SECONDS", "60"))),
        Duration.ofSeconds(Long.parseLong(env("AGGREGATOR_GRACE_SECONDS", "10"))));

    KafkaStreams streams = new KafkaStreams(topology, props);
    CountDownLatch done = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      streams.close(Duration.ofSeconds(10));
      done.countDown();
    }));
    streams.start();
    done.await();
  }
}
