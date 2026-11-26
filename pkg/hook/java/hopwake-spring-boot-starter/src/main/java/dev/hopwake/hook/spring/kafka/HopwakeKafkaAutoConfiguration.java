package dev.hopwake.hook.spring.kafka;

import dev.hopwake.hook.spring.HopwakeAutoConfiguration;
import dev.hopwake.hook.spring.HopwakeProperties;
import dev.hopwake.hook.spring.Hooks;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

/** Marks the topics in hopwake.kafka.marks, when Spring Kafka is on the class path. */
@AutoConfiguration(after = HopwakeAutoConfiguration.class)
@ConditionalOnClass(KafkaTemplate.class)
public class HopwakeKafkaAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    public KafkaReadingResolver hopwakeKafkaReadingResolver(HopwakeProperties props) {
        return new HeaderReadingResolver(props.getKafka());
    }

    @Bean
    @ConditionalOnMissingBean
    public TopicMarks hopwakeTopicMarks(Hooks hooks, KafkaReadingResolver resolver, HopwakeProperties props) {
        return new TopicMarks(hooks, resolver, props);
    }

    // Static, so the post-processor does not pull the rest of this configuration in early.
    @Bean
    static KafkaMarksPostProcessor hopwakeKafkaMarksPostProcessor(ObjectProvider<TopicMarks> marks) {
        return new KafkaMarksPostProcessor(marks);
    }
}
