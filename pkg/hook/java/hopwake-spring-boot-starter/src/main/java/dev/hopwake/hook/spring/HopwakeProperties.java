package dev.hopwake.hook.spring;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Hopwake hook settings. Anything left unset falls back to the environment, as every hook does:
 * HOPWAKE_TENANT, HOPWAKE_OTLP_ENDPOINT, HOPWAKE_TIME_PRECISION and HOPWAKE_API.
 */
@ConfigurationProperties(prefix = "hopwake")
public class HopwakeProperties {
    /** Whether hooks send anything. With false, every hook is a no-op. */
    private boolean enabled = true;
    /** The tenant, as in the rules file. */
    private String tenant;
    /** Hopwake's OTLP/HTTP endpoint, without /v1/traces. */
    private String endpoint;
    /** The service.name on the spans; default spring.application.name. */
    private String service;
    /** Must match time_precision in the rules file. */
    private Duration timePrecision;
    /** The journey service's base URL, for marks made under an alias. */
    private String api;
    private final Kafka kafka = new Kafka();

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public String getTenant() { return tenant; }
    public void setTenant(String tenant) { this.tenant = tenant; }
    public String getEndpoint() { return endpoint; }
    public void setEndpoint(String endpoint) { this.endpoint = endpoint; }
    public String getService() { return service; }
    public void setService(String service) { this.service = service; }
    public Duration getTimePrecision() { return timePrecision; }
    public void setTimePrecision(Duration timePrecision) { this.timePrecision = timePrecision; }
    public String getApi() { return api; }
    public void setApi(String api) { this.api = api; }
    public Kafka getKafka() { return kafka; }

    /** Marks made by the starter itself, around @KafkaListener methods and KafkaTemplate sends. */
    public static class Kafka {
        /** Which topics to mark, and as which hop. */
        private List<TopicMark> marks = new ArrayList<>();
        /** For the default reading resolver: the header that names the device; empty takes the record key. */
        private String deviceHeader;
        /** For the default reading resolver: the header with the sequence number. */
        private String seqHeader;
        /** For the default reading resolver: the header with a boot counter. */
        private String bootHeader;
        /** For the default reading resolver: the header that names the message type. */
        private String typeHeader;
        /** For the default reading resolver: the header with the reading's own time (epoch millis or RFC 3339). */
        private String timeHeader;

        public List<TopicMark> getMarks() { return marks; }
        public void setMarks(List<TopicMark> marks) { this.marks = marks; }
        public String getDeviceHeader() { return deviceHeader; }
        public void setDeviceHeader(String deviceHeader) { this.deviceHeader = deviceHeader; }
        public String getSeqHeader() { return seqHeader; }
        public void setSeqHeader(String seqHeader) { this.seqHeader = seqHeader; }
        public String getBootHeader() { return bootHeader; }
        public void setBootHeader(String bootHeader) { this.bootHeader = bootHeader; }
        public String getTypeHeader() { return typeHeader; }
        public void setTypeHeader(String typeHeader) { this.typeHeader = typeHeader; }
        public String getTimeHeader() { return timeHeader; }
        public void setTimeHeader(String timeHeader) { this.timeHeader = timeHeader; }
    }

    /** One topic the starter marks. */
    public static class TopicMark {
        /** The Kafka topic. */
        private String topic;
        /** The hop name, matching a hook source in the rules file, e.g. hook:measurement-service.received. */
        private String hop;
        /** consume: a record a listener handled without an exception; send: a record the broker acknowledged. */
        private When on = When.CONSUME;

        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        public String getHop() { return hop; }
        public void setHop(String hop) { this.hop = hop; }
        public When getOn() { return on; }
        public void setOn(When on) { this.on = on; }
    }

    /** When a topic mark is made. */
    public enum When { CONSUME, SEND }
}
