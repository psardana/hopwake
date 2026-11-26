package dev.hopwake.hook.spring.kafka;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.kafka.config.AbstractKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchInterceptor;
import org.springframework.kafka.listener.CompositeBatchInterceptor;
import org.springframework.kafka.listener.CompositeRecordInterceptor;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.kafka.support.CompositeProducerListener;
import org.springframework.kafka.support.ProducerListener;

/**
 * Adds the marking interceptors to every listener container factory and the marking producer
 * listener to every KafkaTemplate, next to whatever the application set there already.
 */
class KafkaMarksPostProcessor implements BeanPostProcessor {
    private final ObjectProvider<TopicMarks> marks;

    KafkaMarksPostProcessor(ObjectProvider<TopicMarks> marks) {
        this.marks = marks;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Object postProcessAfterInitialization(Object bean, String name) {
        if (bean instanceof AbstractKafkaListenerContainerFactory) {
            TopicMarks m = marks.getIfAvailable();
            if (m == null || !m.marksConsumes()) {
                return bean;
            }
            AbstractKafkaListenerContainerFactory f = (AbstractKafkaListenerContainerFactory) bean;
            // The factory has no getters for these; keep what the application set.
            DirectFieldAccessor fields = new DirectFieldAccessor(f);
            RecordInterceptor existingRecord = (RecordInterceptor) fields.getPropertyValue("recordInterceptor");
            BatchInterceptor existingBatch = (BatchInterceptor) fields.getPropertyValue("batchInterceptor");
            RecordInterceptor rec = new MarkingInterceptors.Record(m);
            BatchInterceptor bat = new MarkingInterceptors.Batch(m);
            f.setRecordInterceptor(existingRecord == null ? rec : new CompositeRecordInterceptor(existingRecord, rec));
            f.setBatchInterceptor(existingBatch == null ? bat : new CompositeBatchInterceptor(existingBatch, bat));
        } else if (bean instanceof KafkaTemplate) {
            TopicMarks m = marks.getIfAvailable();
            if (m == null || !m.marksSends()) {
                return bean;
            }
            KafkaTemplate t = (KafkaTemplate) bean;
            ProducerListener existing = (ProducerListener) new DirectFieldAccessor(t).getPropertyValue("producerListener");
            ProducerListener ours = new MarkingInterceptors.Producer(m);
            if (existing == null) {
                t.setProducerListener(ours);
            } else {
                // Keep the application's listener, or Spring's default that logs failed sends.
                CompositeProducerListener c = new CompositeProducerListener(existing);
                c.addDelegate(ours);
                t.setProducerListener(c);
            }
        }
        return bean;
    }
}
