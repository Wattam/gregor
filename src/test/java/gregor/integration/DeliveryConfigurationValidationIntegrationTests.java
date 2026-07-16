package gregor.integration;

import gregor.Application;
import gregor.config.properties.KafkaTopicProperties;
import gregor.config.properties.WebhookProperties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

class DeliveryConfigurationValidationIntegrationTests {

    @Test
    void startsWithDeliveryDefaultsAndEnvironmentOverrides() {
        try (ConfigurableApplicationContext context = this.start()) {
            WebhookProperties webhook = context.getBean(WebhookProperties.class);
            KafkaTopicProperties topics = context.getBean(KafkaTopicProperties.class);

            Assertions.assertThat(webhook.maximumAttempts()).isEqualTo(4);
            Assertions.assertThat(topics.retryRetention()).isEqualTo(Duration.ofDays(7));
            Assertions.assertThat(topics.deadLetterRetention()).isEqualTo(Duration.ofDays(30));
        }

        try (ConfigurableApplicationContext context = this.start(
                "--WEBHOOK_MAXIMUM_ATTEMPTS=2",
                "--KAFKA_RETRY_TOPIC_RETENTION=8d",
                "--KAFKA_DEAD_LETTER_TOPIC_RETENTION=31d"
        )) {
            WebhookProperties webhook = context.getBean(WebhookProperties.class);
            KafkaTopicProperties topics = context.getBean(KafkaTopicProperties.class);

            Assertions.assertThat(webhook.maximumAttempts()).isEqualTo(2);
            Assertions.assertThat(topics.retryRetention()).isEqualTo(Duration.ofDays(8));
            Assertions.assertThat(topics.deadLetterRetention()).isEqualTo(Duration.ofDays(31));
        }
    }

    @Test
    void acceptsMaximumAttemptValuesFromOneThroughFour() {
        for (int maximumAttempts : List.of(1, 2, 3, 4)) {
            try (ConfigurableApplicationContext context = this.start(
                    "--gregor.webhook.maximum-attempts=" + maximumAttempts
            )) {
                Assertions.assertThat(context.getBean(WebhookProperties.class).maximumAttempts())
                        .isEqualTo(maximumAttempts);
            }
        }
    }

    @Test
    void rejectsMaximumAttemptValuesOutsideOneThroughFour() {
        this.assertStartupFails("--gregor.webhook.maximum-attempts=0");
        this.assertStartupFails("--gregor.webhook.maximum-attempts=5");
    }

    @Test
    void acceptsOnlyRetryRetentionGreaterThanOneHour() {
        try (ConfigurableApplicationContext context = this.start(
                "--gregor.kafka.topics.retry-retention=3600001ms"
        )) {
            Assertions.assertThat(context.getBean(KafkaTopicProperties.class).retryRetention())
                    .isEqualTo(Duration.ofMillis(3_600_001));
        }

        this.assertStartupFails("--gregor.kafka.topics.retry-retention=1h");
        this.assertStartupFails("--gregor.kafka.topics.retry-retention=59m");
    }

    @Test
    void acceptsOnlyPositiveDeadLetterRetention() {
        try (ConfigurableApplicationContext context = this.start(
                "--gregor.kafka.topics.dead-letter-retention=1ms"
        )) {
            Assertions.assertThat(context.getBean(KafkaTopicProperties.class).deadLetterRetention())
                    .isEqualTo(Duration.ofMillis(1));
        }

        this.assertStartupFails("--gregor.kafka.topics.dead-letter-retention=0s");
        this.assertStartupFails("--gregor.kafka.topics.dead-letter-retention=-1ms");
    }

    private void assertStartupFails(String property) {
        Throwable failure = Assertions.catchThrowable(() -> {
            try (ConfigurableApplicationContext ignored = this.start(property)) {
                // A valid context is closed before the assertion reports that startup unexpectedly succeeded.
            }
        });

        Assertions.assertThat(failure).isNotNull();
    }

    private ConfigurableApplicationContext start(String... propertyOverrides) {
        SpringApplication application = new SpringApplication(Application.class);
        application.setWebApplicationType(WebApplicationType.NONE);
        List<String> arguments = new ArrayList<>(List.of(
                "--spring.datasource.url=jdbc:h2:mem:configuration-" + UUID.randomUUID(),
                "--spring.datasource.username=sa",
                "--spring.datasource.password=",
                "--spring.jpa.hibernate.ddl-auto=create-drop",
                "--spring.kafka.admin.auto-create=false",
                "--camel.springboot.auto-startup=false",
                "--logging.level.root=OFF"
        ));
        arguments.addAll(Arrays.asList(propertyOverrides));
        return application.run(arguments.toArray(String[]::new));
    }
}
