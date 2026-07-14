package gregor.config;

import gregor.config.properties.KafkaTopicProperties;
import gregor.route.WebhookRoute;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@AllArgsConstructor
@Configuration(proxyBeanMethods = false)
public class KafkaConfiguration {

    private final KafkaTopicProperties kafkaTopicProperties;

    @Bean
    public NewTopic webhookEventsTopic() {
        return this.newTopicWithDeleteCleanupPolicy(WebhookRoute.WEBHOOK_EVENT_TOPIC, this.dayToMs(7));
    }

    @Bean
    public NewTopic persistDeliveryAttemptTopic() {
        return this.newTopicWithDeleteCleanupPolicy(WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC, this.dayToMs(30));
    }

    private String dayToMs(int days) {
        return Long.toString((long) days * 24 * 60 * 60 * 1000);
    }

    private NewTopic newTopicWithDeleteCleanupPolicy(String name, String retention) {
        return TopicBuilder
                .name(name)
                .partitions(this.kafkaTopicProperties.defaultPartitions())
                .replicas(this.kafkaTopicProperties.replicationFactor())
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, retention)
                .build();
    }
}
