package gregor.config;

import gregor.avro.ApicurioWebhookEnvelopeReader;
import gregor.avro.WebhookEnvelopeCodec;
import gregor.config.properties.KafkaTopicProperties;
import gregor.route.WebhookRoute;
import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.vertx.core.Vertx;
import lombok.AllArgsConstructor;
import org.apache.camel.Ordered;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.component.kafka.consumer.DefaultKafkaManualSyncCommit;
import org.apache.camel.component.kafka.consumer.KafkaManualCommitFactory;
import org.apache.camel.spi.ComponentCustomizer;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.TopicBuilder;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor
@Configuration(proxyBeanMethods = false)
public class KafkaConfiguration {

    private final KafkaTopicProperties kafkaTopicProperties;

    @Bean(destroyMethod = "close")
    public Vertx registryVertx() {
        return Vertx.vertx();
    }

    @Bean
    public ComponentCustomizer registryVertxCustomizer(Vertx registryVertx) {
        return ComponentCustomizer.builder(KafkaComponent.class).withOrder(Ordered.LOWEST).build(kafkaComponent -> kafkaComponent.getConfiguration().getAdditionalProperties().put(SchemaResolverConfig.VERTX_INSTANCE, registryVertx));
    }

    @Lazy
    @Bean(destroyMethod = "close")
    public ApicurioWebhookEnvelopeReader apicurioWebhookEnvelopeReader(Environment environment, Vertx registryVertx, WebhookEnvelopeCodec webhookEnvelopeCodec) {
        Map<String, Object> serdeProperties = new HashMap<>(Binder.get(environment).bind("camel.component.kafka.additional-properties", Bindable.mapOf(String.class, Object.class)).orElseGet(Map::of));
        serdeProperties.put(SchemaResolverConfig.VERTX_INSTANCE, registryVertx);
        return new ApicurioWebhookEnvelopeReader(webhookEnvelopeCodec, serdeProperties);
    }

    @Bean
    public KafkaManualCommitFactory kafkaManualCommitFactory() {
        return DefaultKafkaManualSyncCommit::new;
    }

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
        return this.newTopicWithDeleteCleanupPolicy(WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC, this.kafkaTopicProperties.retryRetention());
    }

    @Bean
    public NewTopic webhookEventsRetry10mTopic() {
        return this.newTopicWithDeleteCleanupPolicy(WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC, this.kafkaTopicProperties.retryRetention());
    }

    @Bean
    public NewTopic webhookEventsRetry1hTopic() {
        return this.newTopicWithDeleteCleanupPolicy(WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC, this.kafkaTopicProperties.retryRetention());
    }

    @Bean
    public NewTopic webhookEventsDeadLetterTopic() {
        return this.newTopicWithDeleteCleanupPolicy(WebhookRoute.WEBHOOK_EVENT_DEAD_LETTER_TOPIC, this.kafkaTopicProperties.deadLetterRetention());
    }

    private NewTopic newTopicWithDeleteCleanupPolicy(String topicName, Duration retention) {
        return TopicBuilder
                .name(topicName)
                .partitions(this.kafkaTopicProperties.defaultPartitions())
                .replicas(this.kafkaTopicProperties.replicationFactor())
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, Long.toString(retention.toMillis()))
                .build();
    }
}
