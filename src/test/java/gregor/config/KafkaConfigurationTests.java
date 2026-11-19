package gregor.config;

import gregor.config.properties.KafkaTopicProperties;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.assertj.core.api.Assertions;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

class KafkaConfigurationTests {

    @Test
    void appliesNondefaultTopicSettings() {
        KafkaTopicProperties kafkaTopicProperties = new KafkaTopicProperties(7, 2, Duration.ofHours(2), Duration.ofDays(3));
        Map<NewTopic, String> expectedRetentions = getExpectedRetentions(kafkaTopicProperties);

        expectedRetentions.forEach((newTopic, retention) -> {
            Assertions.assertThat(newTopic.numPartitions()).as("%s partitions", newTopic.name()).isEqualTo(7);
            Assertions.assertThat(newTopic.replicationFactor()).as("%s replicas", newTopic.name()).isEqualTo((short) 2);
            Assertions.assertThat(newTopic.configs()).as("%s retention", newTopic.name()).containsEntry(TopicConfig.RETENTION_MS_CONFIG, retention);
        });
    }

    private static @NonNull Map<NewTopic, String> getExpectedRetentions(KafkaTopicProperties kafkaTopicProperties) {
        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(kafkaTopicProperties);
        return Map.of(
                kafkaConfiguration.webhookEventsTopic(), "604800000",
                kafkaConfiguration.persistDeliveryAttemptTopic(), "2592000000",
                kafkaConfiguration.webhookEventsRetry1mTopic(), "7200000",
                kafkaConfiguration.webhookEventsRetry10mTopic(), "7200000",
                kafkaConfiguration.webhookEventsRetry1hTopic(), "7200000",
                kafkaConfiguration.webhookEventsDeadLetterTopic(), "259200000"
        );
    }
}
