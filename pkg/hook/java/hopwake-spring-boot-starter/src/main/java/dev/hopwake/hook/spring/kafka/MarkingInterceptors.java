package dev.hopwake.hook.spring.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.listener.BatchInterceptor;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.kafka.support.ProducerListener;

/** The interceptors and producer listener the starter adds. Each marks only on success. */
final class MarkingInterceptors {
    private MarkingInterceptors() {
    }

    static final class Record implements RecordInterceptor<Object, Object> {
        private final TopicMarks marks;

        Record(TopicMarks marks) {
            this.marks = marks;
        }

        @Override
        public ConsumerRecord<Object, Object> intercept(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
            return record;
        }

        @Override
        public void success(ConsumerRecord<Object, Object> r, Consumer<Object, Object> consumer) {
            marks.consumed(r.topic(), r.key(), r.value(), r.headers());
        }
    }

    static final class Batch implements BatchInterceptor<Object, Object> {
        private final TopicMarks marks;

        Batch(TopicMarks marks) {
            this.marks = marks;
        }

        @Override
        public ConsumerRecords<Object, Object> intercept(ConsumerRecords<Object, Object> records, Consumer<Object, Object> consumer) {
            return records;
        }

        @Override
        public void success(ConsumerRecords<Object, Object> records, Consumer<Object, Object> consumer) {
            for (ConsumerRecord<Object, Object> r : records) {
                marks.consumed(r.topic(), r.key(), r.value(), r.headers());
            }
        }
    }

    static final class Producer implements ProducerListener<Object, Object> {
        private final TopicMarks marks;

        Producer(TopicMarks marks) {
            this.marks = marks;
        }

        @Override
        public void onSuccess(ProducerRecord<Object, Object> r, RecordMetadata metadata) {
            marks.sent(r.topic(), r.key(), r.value(), r.headers());
        }
    }
}
