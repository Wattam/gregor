package gregor.config;

import gregor.config.properties.KafkaTopicProperties;
import gregor.config.properties.WebhookProperties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.validation.autoconfigure.ValidationAutoConfiguration;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.List;

class ConfigurationPropertiesTests {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withInitializer(new ConfigDataApplicationContextInitializer())
            .withConfiguration(AutoConfigurations.of(
                    ConfigurationPropertiesAutoConfiguration.class,
                    ValidationAutoConfiguration.class
            ))
            .withUserConfiguration(PropertiesConfiguration.class)
            .withPropertyValues("spring.config.location=file:./src/main/resources/application.yaml");

    @Test
    void bindsProductionDefaults() {
        this.contextRunner.run(context -> {
            Assertions.assertThat(context.getStartupFailure()).isNull();
            Assertions.assertThat(context.getBean(WebhookProperties.class).maximumAttempts()).isEqualTo(4);
            KafkaTopicProperties kafkaTopics = context.getBean(KafkaTopicProperties.class);
            Assertions.assertThat(kafkaTopics.retryRetention()).isEqualTo(Duration.ofDays(7));
            Assertions.assertThat(kafkaTopics.deadLetterRetention()).isEqualTo(Duration.ofDays(30));
        });
    }

    @Test
    void bindsEnvironmentOverrides() {
        this.contextRunner.withSystemProperties(
                "WEBHOOK_MAXIMUM_ATTEMPTS=1",
                "KAFKA_RETRY_TOPIC_RETENTION=2h",
                "KAFKA_DEAD_LETTER_TOPIC_RETENTION=3d"
        ).run(context -> {
            Assertions.assertThat(context.getStartupFailure()).isNull();
            Assertions.assertThat(context.getBean(WebhookProperties.class).maximumAttempts()).isEqualTo(1);
            KafkaTopicProperties kafkaTopics = context.getBean(KafkaTopicProperties.class);
            Assertions.assertThat(kafkaTopics.retryRetention()).isEqualTo(Duration.ofHours(2));
            Assertions.assertThat(kafkaTopics.deadLetterRetention()).isEqualTo(Duration.ofDays(3));
        });
    }

    @Test
    void acceptsMaximumAttemptsFromOneThroughFour() {
        for (int maximumAttempts = 1; maximumAttempts <= 4; maximumAttempts++) {
            int expectedMaximumAttempts = maximumAttempts;
            this.contextRunner
                    .withPropertyValues("gregor.webhook.maximum-attempts=" + expectedMaximumAttempts)
                    .run(context -> {
                        Assertions.assertThat(context.getStartupFailure()).isNull();
                        Assertions.assertThat(context.getBean(WebhookProperties.class).maximumAttempts())
                                .isEqualTo(expectedMaximumAttempts);
                    });
        }
    }

    @Test
    void rejectsMaximumAttemptsOutsideOneThroughFour() {
        for (int maximumAttempts : List.of(0, 5)) {
            this.contextRunner
                    .withPropertyValues("gregor.webhook.maximum-attempts=" + maximumAttempts)
                    .run(context -> Assertions.assertThat(context.getStartupFailure())
                            .hasRootCauseInstanceOf(BindValidationException.class));
        }
    }

    @Test
    void acceptsRetentionDurationsAboveTheirExclusiveMinimums() {
        this.contextRunner.withPropertyValues(
                "gregor.kafka.topics.retry-retention=3600001ms",
                "gregor.kafka.topics.dead-letter-retention=1ms"
        ).run(context -> {
            Assertions.assertThat(context.getStartupFailure()).isNull();
            KafkaTopicProperties kafkaTopics = context.getBean(KafkaTopicProperties.class);
            Assertions.assertThat(kafkaTopics.retryRetention()).isEqualTo(Duration.ofMillis(3_600_001L));
            Assertions.assertThat(kafkaTopics.deadLetterRetention()).isEqualTo(Duration.ofMillis(1L));
        });
    }

    @Test
    void rejectsRetryRetentionAtOrBelowOneHour() {
        for (String retryRetention : List.of("-1ms", "0ms", "1h")) {
            this.contextRunner
                    .withPropertyValues("gregor.kafka.topics.retry-retention=" + retryRetention)
                    .run(context -> Assertions.assertThat(context.getStartupFailure())
                            .hasRootCauseInstanceOf(BindValidationException.class));
        }
    }

    @Test
    void rejectsNonPositiveDeadLetterRetention() {
        for (String deadLetterRetention : List.of("-1ms", "0ms")) {
            this.contextRunner
                    .withPropertyValues("gregor.kafka.topics.dead-letter-retention=" + deadLetterRetention)
                    .run(context -> Assertions.assertThat(context.getStartupFailure())
                            .hasRootCauseInstanceOf(BindValidationException.class));
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties({WebhookProperties.class, KafkaTopicProperties.class})
    private static class PropertiesConfiguration {
    }
}
