package dev.hopwake.hook.spring.kafka;

import dev.hopwake.hook.Op;
import dev.hopwake.hook.Reading;
import dev.hopwake.hook.spring.HopwakeProperties;
import dev.hopwake.hook.spring.Hooks;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.header.Headers;

/** Marks Kafka records on the topics named in hopwake.kafka.marks. */
public class TopicMarks {
    private final Hooks hooks;
    private final KafkaReadingResolver resolver;
    private final Map<String, String> consumed = new HashMap<>();
    private final Map<String, String> sent = new HashMap<>();

    public TopicMarks(Hooks hooks, KafkaReadingResolver resolver, HopwakeProperties props) {
        this.hooks = hooks;
        this.resolver = resolver;
        for (HopwakeProperties.TopicMark m : props.getKafka().getMarks()) {
            if (m.getTopic() == null || m.getHop() == null) {
                throw new IllegalArgumentException("hopwake.kafka.marks: every mark needs a topic and a hop");
            }
            (m.getOn() == HopwakeProperties.When.SEND ? sent : consumed).put(m.getTopic(), m.getHop());
        }
    }

    boolean marksConsumes() {
        return hooks.enabled() && !consumed.isEmpty();
    }

    boolean marksSends() {
        return hooks.enabled() && !sent.isEmpty();
    }

    void consumed(String topic, Object key, Object value, Headers headers) {
        mark(consumed.get(topic), Op.RECEIVE, topic, key, value, headers);
    }

    void sent(String topic, Object key, Object value, Headers headers) {
        mark(sent.get(topic), Op.ROUTE, topic, key, value, headers);
    }

    private void mark(String hop, Op op, String topic, Object key, Object value, Headers headers) {
        if (hop == null) {
            return;
        }
        try {
            Reading r = resolver.resolve(topic, key, value, headers);
            if (r != null) {
                hooks.mark(hop, op, r);
            }
        } catch (RuntimeException e) {
            // A record the resolver cannot read is simply not marked.
        }
    }
}
