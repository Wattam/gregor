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
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

class KafkaConfigurationTests {

    @Test
    void topicIdentifiersArePublicStaticFinalConstantsOnWebhookRoute() throws ReflectiveOperationException {
        Assertions.assertThat(WebhookRoute.class.getField("WEBHOOK_EVENT_TOPIC").getModifiers())
                .matches(modifiers -> Modifier.isPublic(modifiers)
                        && Modifier.isStatic(modifiers)
                        && Modifier.isFinal(modifiers));
        Assertions.assertThat(WebhookRoute.class.getField("PERSIST_DELIVERY_ATTEMPT_TOPIC").getModifiers())
                .matches(modifiers -> Modifier.isPublic(modifiers)
                        && Modifier.isStatic(modifiers)
                        && Modifier.isFinal(modifiers));
        Assertions.assertThat(WebhookRoute.WEBHOOK_EVENT_TOPIC).isEqualTo("webhook-events");
        Assertions.assertThat(WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC).isEqualTo("persist-delivery-attempt");
    }

    @Test
    void declaresOnlyPhaseOneTopicsWithExpectedConfiguration() {
        KafkaTopicProperties kafkaTopicProperties = new KafkaTopicProperties(12, 1);
        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(kafkaTopicProperties);
        NewTopic webhookEvents = kafkaConfiguration.webhookEventsTopic();
        NewTopic deliveryAttempts = kafkaConfiguration.persistDeliveryAttemptTopic();

        Assertions.assertThat(webhookEvents.name()).isEqualTo(WebhookRoute.WEBHOOK_EVENT_TOPIC);
        Assertions.assertThat(webhookEvents.numPartitions()).isEqualTo(12);
        Assertions.assertThat(webhookEvents.replicationFactor()).isEqualTo((short) 1);
        Assertions.assertThat(webhookEvents.configs())
                .containsEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .containsEntry(TopicConfig.RETENTION_MS_CONFIG, "604800000");
        Assertions.assertThat(deliveryAttempts.name()).isEqualTo(WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC);
        Assertions.assertThat(deliveryAttempts.numPartitions()).isEqualTo(12);
        Assertions.assertThat(deliveryAttempts.replicationFactor()).isEqualTo((short) 1);
        Assertions.assertThat(deliveryAttempts.configs())
                .containsEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .containsEntry(TopicConfig.RETENTION_MS_CONFIG, "2592000000");

        Set<String> beanMethods = Arrays.stream(KafkaConfiguration.class.getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(Bean.class))
                .map(Method::getName)
                .collect(Collectors.toSet());
        Assertions.assertThat(beanMethods)
                .containsExactlyInAnyOrder("webhookEventsTopic", "persistDeliveryAttemptTopic");
    }
}
