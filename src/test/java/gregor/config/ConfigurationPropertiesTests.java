package gregor.config;

import gregor.config.properties.KafkaConsumerProperties;
import gregor.config.properties.KafkaTopicProperties;
import gregor.config.properties.WebhookProperties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.validation.autoconfigure.ValidationAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.io.FileSystemResource;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
            KafkaConsumerProperties kafkaConsumer = context.getBean(KafkaConsumerProperties.class);
            Assertions.assertThat(kafkaConsumer.maxPollIntervalMs()).isEqualTo(4_500_000);
            Assertions.assertThat(kafkaConsumer.maximumPermittedWait()).isEqualTo(Duration.ofMinutes(70));
            KafkaTopicProperties kafkaTopics = context.getBean(KafkaTopicProperties.class);
            Assertions.assertThat(kafkaTopics.retryRetention()).isEqualTo(Duration.ofDays(7));
            Assertions.assertThat(kafkaTopics.deadLetterRetention()).isEqualTo(Duration.ofDays(30));
        });
    }

    @Test
    void bindsEnvironmentOverrides() {
        this.contextRunner.withSystemProperties(
                "WEBHOOK_MAXIMUM_ATTEMPTS=1",
                "KAFKA_MAX_POLL_INTERVAL_MS=4200000",
                "KAFKA_RETRY_TOPIC_RETENTION=2h",
                "KAFKA_DEAD_LETTER_TOPIC_RETENTION=3d"
        ).run(context -> {
            Assertions.assertThat(context.getStartupFailure()).isNull();
            Assertions.assertThat(context.getBean(WebhookProperties.class).maximumAttempts()).isEqualTo(1);
            KafkaConsumerProperties kafkaConsumer = context.getBean(KafkaConsumerProperties.class);
            Assertions.assertThat(kafkaConsumer.maxPollIntervalMs()).isEqualTo(4_200_000);
            Assertions.assertThat(kafkaConsumer.maximumPermittedWait()).isEqualTo(Duration.ofMinutes(65));
            KafkaTopicProperties kafkaTopics = context.getBean(KafkaTopicProperties.class);
            Assertions.assertThat(kafkaTopics.retryRetention()).isEqualTo(Duration.ofHours(2));
            Assertions.assertThat(kafkaTopics.deadLetterRetention()).isEqualTo(Duration.ofDays(3));
        });
    }

    @Test
    void acceptsTheMinimumKafkaMaxPollInterval() {
        this.contextRunner
                .withPropertyValues("camel.component.kafka.max-poll-interval-ms=3900001")
                .run(context -> {
                    Assertions.assertThat(context.getStartupFailure()).isNull();
                    KafkaConsumerProperties kafkaConsumer = context.getBean(KafkaConsumerProperties.class);
                    Assertions.assertThat(kafkaConsumer.maxPollIntervalMs()).isEqualTo(3_900_001);
                    Assertions.assertThat(kafkaConsumer.maximumPermittedWait())
                            .isEqualTo(Duration.ofMillis(3_600_001));
                });
    }

    @Test
    void rejectsNullAndValuesBelowTheMinimumKafkaMaxPollInterval() {
        for (String maxPollInterval : List.of("", "3900000")) {
            this.contextRunner
                    .withPropertyValues("camel.component.kafka.max-poll-interval-ms=" + maxPollInterval)
                    .run(context -> Assertions.assertThat(context.getStartupFailure())
                            .hasRootCauseInstanceOf(BindValidationException.class));
        }
    }

    @Test
    void documentsEveryCamelPropertyUsedInProduction() throws IOException {
        Set<String> documented = new HashSet<>();
        new ObjectMapper().readTree(Files.readString(Path.of(
                "src/main/resources/META-INF/additional-spring-configuration-metadata.json"
        ))).get("properties").forEach(property -> documented.add(property.get("name").asString()));
        List<String> configured = new YamlPropertySourceLoader()
                .load("production", new FileSystemResource("src/main/resources/application.yaml"))
                .stream()
                .flatMap(source -> Arrays.stream(((EnumerablePropertySource<?>) source).getPropertyNames()))
                .filter(name -> name.startsWith("camel."))
                .map(name -> name.replaceFirst("\\.?\\[.*", ""))
                .distinct()
                .toList();

        Assertions.assertThat(documented).containsAll(configured);
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
    @EnableConfigurationProperties({
            WebhookProperties.class,
            KafkaConsumerProperties.class,
            KafkaTopicProperties.class
    })
    private static class PropertiesConfiguration {
    }
}
