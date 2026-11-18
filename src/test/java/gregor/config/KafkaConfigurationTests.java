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
        KafkaTopicProperties properties = new KafkaTopicProperties(7, 2, Duration.ofHours(2), Duration.ofDays(3));
        Map<NewTopic, String> expectedRetentions = getExpectedRetentions(properties);

        expectedRetentions.forEach((topic, retention) -> {
            Assertions.assertThat(topic.numPartitions()).as("%s partitions", topic.name()).isEqualTo(7);
            Assertions.assertThat(topic.replicationFactor()).as("%s replicas", topic.name()).isEqualTo((short) 2);
            Assertions.assertThat(topic.configs()).as("%s retention", topic.name())
                    .containsEntry(TopicConfig.RETENTION_MS_CONFIG, retention);
        });
    }

    private static @NonNull Map<NewTopic, String> getExpectedRetentions(KafkaTopicProperties properties) {
        KafkaConfiguration configuration = new KafkaConfiguration(properties);
        return Map.of(
                configuration.webhookEventsTopic(), "604800000",
                configuration.persistDeliveryAttemptTopic(), "2592000000",
                configuration.webhookEventsRetry1mTopic(), "7200000",
                configuration.webhookEventsRetry10mTopic(), "7200000",
                configuration.webhookEventsRetry1hTopic(), "7200000",
                configuration.webhookEventsDeadLetterTopic(), "259200000"
        );
    }
}
