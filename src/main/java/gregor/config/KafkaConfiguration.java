package gregor.config;

import gregor.config.properties.KafkaTopicProperties;
import gregor.route.WebhookRoute;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.time.Duration;

@AllArgsConstructor
@Configuration(proxyBeanMethods = false)
public class KafkaConfiguration {

    private final KafkaTopicProperties kafkaTopicProperties;

    @Bean
    public NewTopic webhookEventsTopic() {
        return this.newTopicWithDeleteCleanupPolicy(WebhookRoute.WEBHOOK_EVENT_TOPIC, Duration.ofDays(7));
    }

    @Bean
    public NewTopic persistDeliveryAttemptTopic() {
        return this.newTopicWithDeleteCleanupPolicy(WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC, Duration.ofDays(30));
    }

    @Bean
    public NewTopic webhookEventsRetry1mTopic() {
        return this.newTopicWithDeleteCleanupPolicy(
                WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC,
                this.kafkaTopicProperties.retryRetention()
        );
    }

    @Bean
    public NewTopic webhookEventsRetry10mTopic() {
        return this.newTopicWithDeleteCleanupPolicy(
                WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC,
                this.kafkaTopicProperties.retryRetention()
        );
    }

    @Bean
    public NewTopic webhookEventsRetry1hTopic() {
        return this.newTopicWithDeleteCleanupPolicy(
                WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC,
                this.kafkaTopicProperties.retryRetention()
        );
    }

    @Bean
    public NewTopic webhookEventsDeadLetterTopic() {
        return this.newTopicWithDeleteCleanupPolicy(
                WebhookRoute.WEBHOOK_EVENT_DEAD_LETTER_TOPIC,
                this.kafkaTopicProperties.deadLetterRetention()
        );
    }

    private NewTopic newTopicWithDeleteCleanupPolicy(String name, Duration retention) {
        return TopicBuilder
                .name(name)
                .partitions(this.kafkaTopicProperties.defaultPartitions())
                .replicas(this.kafkaTopicProperties.replicationFactor())
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, Long.toString(retention.toMillis()))
                .build();
    }
}
