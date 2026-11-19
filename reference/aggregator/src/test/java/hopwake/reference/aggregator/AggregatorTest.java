package hopwake.reference.aggregator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

class AggregatorTest {
  private static String reading(String device, String site, double tempK, long pressurePa) {
    return String.format(
        "{\"device\":\"%s\",\"site\":\"%s\",\"temperature_k\":%s,\"pressure_pa\":%d}",
        device, site, tempK, pressurePa);
  }

  @Test
  void emitsMedianPerSiteWhenTheWindowCloses() throws Exception {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
    try (TopologyTestDriver driver = new TopologyTestDriver(
        Aggregator.topology("in", "out", Duration.ofMinutes(1), Duration.ofSeconds(10)), props)) {
      TestInputTopic<String, String> in =
          driver.createInputTopic("in", new StringSerializer(), new StringSerializer());
      TestOutputTopic<String, String> out =
          driver.createOutputTopic("out", new StringDeserializer(), new StringDeserializer());

      Instant t0 = Instant.parse("2026-11-18T10:00:05Z");
      in.pipeInput("a", reading("a", "site-01", 290.0, 100000), t0);
      in.pipeInput("b", reading("b", "site-01", 300.0, 101000), t0.plusSeconds(10));
      in.pipeInput("a", reading("a", "site-01", 295.0, 102000), t0.plusSeconds(20));
      assertTrue(out.isEmpty(), "nothing is emitted before the window closes");

      // A later record moves stream time past the window end plus grace.
      in.pipeInput("c", reading("c", "site-02", 280.0, 99000), t0.plusSeconds(90));
      List<String> results = out.readValuesToList();
      assertEquals(1, results.size());
      JsonNode r = Aggregator.JSON.readTree(results.get(0));
      assertEquals("site-01", r.get("site").asText());
      assertEquals(3, r.get("readings").asInt());
      assertEquals(2, r.get("devices").asInt());
      assertEquals(295.0, r.get("median_temperature_k").asDouble());
      assertEquals(101000, r.get("median_pressure_pa").asLong());
      assertEquals("2026-11-18T10:00:00Z", r.get("window_start").asText());
    }
  }

  @Test
  void medianOfAnEvenCountIsTheMeanOfTheMiddlePair() {
    assertEquals(2.5, Aggregator.median(List.of(4.0, 1.0, 3.0, 2.0)));
  }
}
