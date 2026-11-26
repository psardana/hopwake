package dev.hopwake.hook.spring.kafka;

import dev.hopwake.hook.Reading;
import org.apache.kafka.common.header.Headers;

/**
 * Says which reading a Kafka record carries, so the starter can mark it. Define one as a bean when
 * the reading's identity is in the payload, for example a Protobuf measurement's own time:
 *
 * <pre>
 * &#64;Bean
 * KafkaReadingResolver readings() {
 *     return (topic, key, value, headers) -&gt; value instanceof Ems.Measurement m
 *         ? Reading.time((String) key, "ems.measurements.Measurement", toInstant(m.getMeasurementTime()))
 *         : null;
 * }
 * </pre>
 *
 * Return null for a record that is not a reading Hopwake follows. Without such a bean, the starter
 * reads the reading from headers (see {@link HeaderReadingResolver}).
 */
@FunctionalInterface
public interface KafkaReadingResolver {
    Reading resolve(String topic, Object key, Object value, Headers headers);
}
