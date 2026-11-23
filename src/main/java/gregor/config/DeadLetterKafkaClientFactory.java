package gregor.config;

import org.apache.camel.component.kafka.DefaultKafkaClientFactory;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

final class DeadLetterKafkaClientFactory extends DefaultKafkaClientFactory {

    // Share unacknowledged positions across all clients created for this endpoint.
    private final ConcurrentMap<TopicPartition, Long> pendingOffsets = new ConcurrentHashMap<>();

    @Override
    public KafkaConsumer<Object, Object> getConsumer(Properties properties) {
        return new RetryingKafkaConsumer(properties, this.pendingOffsets);
    }

    private static final class RetryingKafkaConsumer extends KafkaConsumer<Object, Object> {

        private final ConcurrentMap<TopicPartition, Long> pendingOffsets;

        private RetryingKafkaConsumer(Properties properties, ConcurrentMap<TopicPartition, Long> pendingOffsets) {
            super(properties);
            this.pendingOffsets = pendingOffsets;
        }

        @Override
        public ConsumerRecords<Object, Object> poll(Duration timeout) {
            ConsumerRecords<Object, Object> consumerRecords = super.poll(timeout);
            // The route polls at most one record and commits it synchronously after persistence.
            for (TopicPartition topicPartition : consumerRecords.partitions()) {
                this.pendingOffsets.put(topicPartition, consumerRecords.records(topicPartition).getFirst().offset());
            }
            return consumerRecords;
        }

        @Override
        public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
            super.commitSync(offsets, timeout);
            offsets.forEach((topicPartition, offsetAndMetadata) -> this.pendingOffsets.remove(topicPartition, offsetAndMetadata.offset() - 1));
        }

        @Override
        public void subscribe(Collection<String> topics, ConsumerRebalanceListener consumerRebalanceListener) {
            super.subscribe(topics, new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> topicPartitions) {
                    consumerRebalanceListener.onPartitionsRevoked(topicPartitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> topicPartitions) {
                    consumerRebalanceListener.onPartitionsAssigned(topicPartitions);
                    RetryingKafkaConsumer.this.seekPendingOffsets(topicPartitions);
                }

                @Override
                public void onPartitionsLost(Collection<TopicPartition> topicPartitions) {
                    consumerRebalanceListener.onPartitionsLost(topicPartitions);
                }
            });
        }

        @Override
        public void resume(Collection<TopicPartition> topicPartitions) {
            // Camel seeks to broker commits before resuming. A commit beyond the old log end may now be readable.
            this.seekPendingOffsets(topicPartitions);
            super.resume(topicPartitions);
        }

        private void seekPendingOffsets(Collection<TopicPartition> topicPartitions) {
            for (TopicPartition topicPartition : topicPartitions) {
                Long pendingOffset = this.pendingOffsets.get(topicPartition);
                if (Objects.nonNull(pendingOffset)) {
                    this.seek(topicPartition, pendingOffset);
                }
            }
        }
    }
}
