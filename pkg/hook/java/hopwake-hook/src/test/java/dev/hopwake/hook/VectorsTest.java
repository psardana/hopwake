package dev.hopwake.hook;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Every journey ID, time and alias in the shared vectors, which the Go hook writes. */
class VectorsTest {
    @SuppressWarnings("unchecked")
    private static Map<String, Object> vectors() throws IOException {
        return (Map<String, Object>) TestJson.parse(Files.readString(Path.of("../../testdata/vectors.json")));
    }

    @Test
    @SuppressWarnings("unchecked")
    void sequenceIds() throws IOException {
        List<Map<String, String>> seq = (List<Map<String, String>>) vectors().get("seq");
        for (Map<String, String> v : seq) {
            long n = Long.parseUnsignedLong(v.get("seq"));
            assertEquals(v.get("id"), JourneyIds.seq(v.get("tenant"), v.get("device"), v.get("boot"), n), v.toString());
        }
        assertEquals(7, seq.size());
    }

    @Test
    @SuppressWarnings("unchecked")
    void timesAndTimeIds() throws IOException {
        List<Map<String, String>> times = (List<Map<String, String>>) vectors().get("time");
        for (Map<String, String> v : times) {
            Duration p = Duration.ofNanos(Long.parseLong(v.get("precision_ns")));
            String f = HopTimes.format(HopTimes.ofUnixNanos(Long.parseLong(v.get("unix_nanos"))), p);
            assertEquals(v.get("formatted"), f, v.toString());
            assertEquals(v.get("id"), JourneyIds.time(v.get("tenant"), v.get("device"), v.get("type"), f), v.toString());
        }
        assertEquals(12, times.size());
    }

    @Test
    @SuppressWarnings("unchecked")
    void aliases() throws IOException {
        for (Map<String, String> v : (List<Map<String, String>>) vectors().get("alias")) {
            assertEquals(v.get("formatted"), Aliases.format(v.get("kind"), v.get("value")));
        }
    }

    @Test
    void durations() {
        assertEquals(Duration.ofSeconds(1), HopTimes.parseDuration("1s"));
        assertEquals(Duration.ofMillis(500), HopTimes.parseDuration("500ms"));
        assertEquals(Duration.ofSeconds(90), HopTimes.parseDuration("1m30s"));
        assertEquals(Duration.ofNanos(250_000), HopTimes.parseDuration("250us"));
        assertEquals(Duration.ofHours(1), HopTimes.parseDuration("1h"));
        assertEquals(null, HopTimes.parseDuration(""));
    }
}
