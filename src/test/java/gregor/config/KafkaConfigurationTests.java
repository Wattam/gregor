package gregor.config;

import gregor.config.properties.KafkaTopicProperties;
import gregor.route.WebhookRoute;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Bean;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class KafkaConfigurationTests {

    @Test
    void topicIdentifiersArePublicStaticFinalConstantsOnWebhookRoute() throws ReflectiveOperationException {
        for (String fieldName : List.of(
                "WEBHOOK_EVENT_TOPIC",
                "PERSIST_DELIVERY_ATTEMPT_TOPIC",
                "WEBHOOK_EVENT_RETRY_1M_TOPIC",
                "WEBHOOK_EVENT_RETRY_10M_TOPIC",
                "WEBHOOK_EVENT_RETRY_1H_TOPIC",
                "WEBHOOK_EVENT_DEAD_LETTER_TOPIC"
        )) {
            Assertions.assertThat(WebhookRoute.class.getField(fieldName).getModifiers())
                    .matches(modifiers -> Modifier.isPublic(modifiers)
                            && Modifier.isStatic(modifiers)
                            && Modifier.isFinal(modifiers));
        }
        Assertions.assertThat(WebhookRoute.WEBHOOK_EVENT_TOPIC).isEqualTo("webhook-events");
        Assertions.assertThat(WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC).isEqualTo("persist-delivery-attempt");
        Assertions.assertThat(WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC)
                .isEqualTo("webhook-events-retry-1m");
        Assertions.assertThat(WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC)
                .isEqualTo("webhook-events-retry-10m");
        Assertions.assertThat(WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC)
                .isEqualTo("webhook-events-retry-1h");
        Assertions.assertThat(WebhookRoute.WEBHOOK_EVENT_DEAD_LETTER_TOPIC)
                .isEqualTo("webhook-events-dead-letter");
    }

    @Test
    void declaresAllTopicsWithExpectedConfiguration() {
        KafkaTopicProperties kafkaTopicProperties =
                new KafkaTopicProperties(12, 1, Duration.ofHours(2), Duration.ofDays(2));
        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(kafkaTopicProperties);
        NewTopic webhookEvents = kafkaConfiguration.webhookEventsTopic();
        NewTopic deliveryAttempts = kafkaConfiguration.persistDeliveryAttemptTopic();
        NewTopic retry1m = kafkaConfiguration.webhookEventsRetry1mTopic();
        NewTopic retry10m = kafkaConfiguration.webhookEventsRetry10mTopic();
        NewTopic retry1h = kafkaConfiguration.webhookEventsRetry1hTopic();
        NewTopic deadLetter = kafkaConfiguration.webhookEventsDeadLetterTopic();

        Assertions.assertThat(webhookEvents.name()).isEqualTo(WebhookRoute.WEBHOOK_EVENT_TOPIC);
        Assertions.assertThat(webhookEvents.configs())
                .containsEntry(TopicConfig.RETENTION_MS_CONFIG, "604800000");
        Assertions.assertThat(deliveryAttempts.name()).isEqualTo(WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC);
        Assertions.assertThat(deliveryAttempts.configs())
                .containsEntry(TopicConfig.RETENTION_MS_CONFIG, "2592000000");
        Assertions.assertThat(List.of(retry1m, retry10m, retry1h))
                .extracting(NewTopic::name)
                .containsExactly(
                        WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC,
                        WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC,
                        WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC
                );
        Assertions.assertThat(List.of(retry1m, retry10m, retry1h)).allSatisfy(topic ->
                Assertions.assertThat(topic.configs())
                        .containsEntry(TopicConfig.RETENTION_MS_CONFIG, "7200000")
        );
        Assertions.assertThat(deadLetter.name()).isEqualTo(WebhookRoute.WEBHOOK_EVENT_DEAD_LETTER_TOPIC);
        Assertions.assertThat(deadLetter.configs())
                .containsEntry(TopicConfig.RETENTION_MS_CONFIG, "172800000");
        Assertions.assertThat(List.of(webhookEvents, deliveryAttempts, retry1m, retry10m, retry1h, deadLetter))
                .allSatisfy(topic -> {
                    Assertions.assertThat(topic.numPartitions()).isEqualTo(12);
                    Assertions.assertThat(topic.replicationFactor()).isEqualTo((short) 1);
                    Assertions.assertThat(topic.configs())
                            .containsEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
                });

        Set<String> beanMethods = Arrays.stream(KafkaConfiguration.class.getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(Bean.class))
                .map(Method::getName)
                .collect(Collectors.toSet());
        Assertions.assertThat(beanMethods).containsExactlyInAnyOrder(
                "webhookEventsTopic",
                "persistDeliveryAttemptTopic",
                "webhookEventsRetry1mTopic",
                "webhookEventsRetry10mTopic",
                "webhookEventsRetry1hTopic",
                "webhookEventsDeadLetterTopic"
        );
    }
}
