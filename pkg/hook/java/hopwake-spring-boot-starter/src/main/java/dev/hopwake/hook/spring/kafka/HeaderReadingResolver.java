package dev.hopwake.hook.spring.kafka;

import dev.hopwake.hook.Reading;
import dev.hopwake.hook.spring.HopwakeProperties;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/**
 * Finds the reading in record headers: the device in {@code device-header} (or the record key),
 * and either a sequence number in {@code seq-header} or a time in {@code time-header}, with the
 * message type from {@code type-header}.
 */
public class HeaderReadingResolver implements KafkaReadingResolver {
    private final HopwakeProperties.Kafka cfg;

    public HeaderReadingResolver(HopwakeProperties.Kafka cfg) {
        this.cfg = cfg;
    }

    @Override
    public Reading resolve(String topic, Object key, Object value, Headers headers) {
        String device = cfg.getDeviceHeader() != null ? header(headers, cfg.getDeviceHeader()) : keyText(key);
        if (device == null || device.isEmpty()) {
            return null;
        }
        String seq = header(headers, cfg.getSeqHeader());
        if (seq != null) {
            try {
                return Reading.seq(device, header(headers, cfg.getBootHeader()), Long.parseUnsignedLong(seq.trim()));
            } catch (NumberFormatException e) {
                return null;
            }
        }
        String time = header(headers, cfg.getTimeHeader());
        if (time == null) {
            return null;
        }
        Instant at = parseTime(time.trim());
        return at == null ? null : Reading.time(device, header(headers, cfg.getTypeHeader()), at);
    }

    static Instant parseTime(String s) {
        try {
            return Instant.ofEpochMilli(Long.parseLong(s));
        } catch (NumberFormatException e) {
            try {
                return Instant.parse(s);
            } catch (RuntimeException e2) {
                return null;
            }
        }
    }

    /** A header's value as UTF-8 text, or null. */
    public static String header(Headers headers, String name) {
        if (headers == null || name == null || name.isEmpty()) {
            return null;
        }
        Header h = headers.lastHeader(name);
        return h == null || h.value() == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    private static String keyText(Object key) {
        if (key instanceof byte[]) {
            return new String((byte[]) key, StandardCharsets.UTF_8);
        }
        return key == null ? null : key.toString();
    }
}
