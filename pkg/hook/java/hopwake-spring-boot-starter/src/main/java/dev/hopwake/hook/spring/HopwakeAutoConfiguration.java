package dev.hopwake.hook.spring;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/** Makes the {@link Hooks} bean. The Kafka marks are in {@link HopwakeKafkaAutoConfiguration}. */
@AutoConfiguration
@EnableConfigurationProperties(HopwakeProperties.class)
public class HopwakeAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    public Hooks hopwakeHooks(HopwakeProperties props, @Value("${spring.application.name:}") String appName) {
        return new Hooks(props, appName);
    }
}
