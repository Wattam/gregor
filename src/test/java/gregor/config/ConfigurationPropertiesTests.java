package gregor.config;

import gregor.config.properties.KafkaConsumerProperties;
import gregor.config.properties.KafkaTopicProperties;
import gregor.config.properties.WebhookProperties;
import org.apache.commons.lang3.Strings;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.validation.autoconfigure.ValidationAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.io.FileSystemResource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class ConfigurationPropertiesTests {

    private final ApplicationContextRunner applicationContextRunner = new ApplicationContextRunner()
            .withInitializer(new ConfigDataApplicationContextInitializer())
            .withConfiguration(AutoConfigurations.of(ConfigurationPropertiesAutoConfiguration.class, ValidationAutoConfiguration.class))
            .withUserConfiguration(PropertiesConfiguration.class)
            .withPropertyValues("spring.config.location=file:./src/main/resources/application.yaml");

    @Test
    void bindsProductionDefaults() {
        this.applicationContextRunner.run(assertableApplicationContext -> {
            Assertions.assertThat(assertableApplicationContext.getStartupFailure()).isNull();
            Assertions.assertThat(assertableApplicationContext.getBean(WebhookProperties.class).maximumAttempts()).isEqualTo(4);
            KafkaConsumerProperties kafkaConsumerProperties = assertableApplicationContext.getBean(KafkaConsumerProperties.class);
            Assertions.assertThat(kafkaConsumerProperties.maxPollIntervalMs()).isEqualTo(4_500_000);
            Assertions.assertThat(kafkaConsumerProperties.maximumPermittedWait()).isEqualTo(Duration.ofMinutes(70));
            Assertions.assertThat(assertableApplicationContext.getEnvironment().getRequiredProperty("camel.component.kafka.connection-max-idle-ms", Integer.class)).isEqualTo(4_500_000);
            KafkaTopicProperties kafkaTopicProperties = assertableApplicationContext.getBean(KafkaTopicProperties.class);
            Assertions.assertThat(kafkaTopicProperties.retryRetention()).isEqualTo(Duration.ofDays(7));
            Assertions.assertThat(kafkaTopicProperties.deadLetterRetention()).isEqualTo(Duration.ofDays(30));
        });
    }

    @Test
    void requiresAllKafkaAcknowledgementsAndManualOffsetCommitsInProduction() {
        this.applicationContextRunner.run(assertableApplicationContext -> {
            Environment environment = assertableApplicationContext.getEnvironment();
            Assertions.assertThat(environment.getRequiredProperty("camel.component.kafka.request-required-acks")).isEqualTo("all");
            Assertions.assertThat(environment.getRequiredProperty("camel.component.kafka.auto-commit-enable", Boolean.class)).isFalse();
            Assertions.assertThat(environment.getRequiredProperty("camel.component.kafka.allow-manual-commit", Boolean.class)).isTrue();
            Assertions.assertThat(environment.getRequiredProperty("camel.component.kafka.break-on-first-error", Boolean.class)).isTrue();
            Assertions.assertThat(environment.getRequiredProperty("camel.component.kafka.auto-offset-reset")).isEqualTo("earliest");
            Assertions.assertThat(environment.getRequiredProperty("camel.component.kafka.max-poll-records", Integer.class)).isEqualTo(1);
        });
    }

    @Test
    void keepsTheExistingKafkaSchemaRegistryConfigurationSurface() {
        this.applicationContextRunner.run(assertableApplicationContext -> {
            Map<String, String> serdeProperties = Binder.get(assertableApplicationContext.getEnvironment()).bind("camel.component.kafka.additional-properties", Bindable.mapOf(String.class, String.class)).get();
            Assertions.assertThat(serdeProperties)
                    .containsOnlyKeys("apicurio.registry.url", "apicurio.registry.auto-register", "apicurio.registry.use-specific-avro-reader")
                    .containsEntry("apicurio.registry.auto-register", "true")
                    .containsEntry("apicurio.registry.use-specific-avro-reader", "true");
        });
    }

    @Test
    void bindsEnvironmentOverrides() {
        this.applicationContextRunner
                .withSystemProperties("WEBHOOK_MAXIMUM_ATTEMPTS=1", "KAFKA_MAX_POLL_INTERVAL_MS=4200000", "KAFKA_RETRY_TOPIC_RETENTION=2h", "KAFKA_DEAD_LETTER_TOPIC_RETENTION=3d")
                .run(assertableApplicationContext -> {
                    Assertions.assertThat(assertableApplicationContext.getStartupFailure()).isNull();
                    Assertions.assertThat(assertableApplicationContext.getBean(WebhookProperties.class).maximumAttempts()).isEqualTo(1);
                    KafkaConsumerProperties kafkaConsumerProperties = assertableApplicationContext.getBean(KafkaConsumerProperties.class);
                    Assertions.assertThat(kafkaConsumerProperties.maxPollIntervalMs()).isEqualTo(4_200_000);
                    Assertions.assertThat(kafkaConsumerProperties.maximumPermittedWait()).isEqualTo(Duration.ofMinutes(65));
                    Assertions.assertThat(assertableApplicationContext.getEnvironment().getRequiredProperty("camel.component.kafka.connection-max-idle-ms", Integer.class)).isEqualTo(4_200_000);
                    KafkaTopicProperties kafkaTopicProperties = assertableApplicationContext.getBean(KafkaTopicProperties.class);
                    Assertions.assertThat(kafkaTopicProperties.retryRetention()).isEqualTo(Duration.ofHours(2));
                    Assertions.assertThat(kafkaTopicProperties.deadLetterRetention()).isEqualTo(Duration.ofDays(3));
                });
    }

    @Test
    void acceptsTheMinimumKafkaMaxPollInterval() {
        this.applicationContextRunner
                .withPropertyValues("camel.component.kafka.max-poll-interval-ms=3900001")
                .run(assertableApplicationContext -> {
                    Assertions.assertThat(assertableApplicationContext.getStartupFailure()).isNull();
                    KafkaConsumerProperties kafkaConsumerProperties = assertableApplicationContext.getBean(KafkaConsumerProperties.class);
                    Assertions.assertThat(kafkaConsumerProperties.maxPollIntervalMs()).isEqualTo(3_900_001);
                    Assertions.assertThat(kafkaConsumerProperties.maximumPermittedWait()).isEqualTo(Duration.ofMillis(3_600_001));
                });
    }

    @Test
    void rejectsNullAndValuesBelowTheMinimumKafkaMaxPollInterval() {
        for (String maxPollInterval : List.of("", "3900000")) {
            this.applicationContextRunner
                    .withPropertyValues("camel.component.kafka.max-poll-interval-ms=" + maxPollInterval)
                    .run(assertableApplicationContext -> Assertions.assertThat(assertableApplicationContext.getStartupFailure()).hasRootCauseInstanceOf(BindValidationException.class));
        }
    }

    @Test
    void documentsEveryCamelPropertyUsedInProduction() throws IOException {
        Set<String> documentedPropertyNames = new HashSet<>();
        Path metadataPath = Path.of("src/main/resources/META-INF/additional-spring-configuration-metadata.json");
        JsonNode metadataJsonNode = new ObjectMapper().readTree(Files.readString(metadataPath));
        metadataJsonNode.get("properties").forEach(propertyJsonNode -> documentedPropertyNames.add(propertyJsonNode.get("name").asString()));
        List<String> configuredPropertyNames = new YamlPropertySourceLoader()
                .load("production", new FileSystemResource("src/main/resources/application.yaml"))
                .stream()
                .flatMap(propertySource -> Arrays.stream(((EnumerablePropertySource<?>) propertySource).getPropertyNames()))
                .filter(name -> Strings.CS.startsWith(name, "camel."))
                .map(name -> name.replaceFirst("\\.?\\[.*", ""))
                .distinct()
                .toList();

        Assertions.assertThat(documentedPropertyNames).containsAll(configuredPropertyNames);
    }

    @Test
    void acceptsMaximumAttemptsFromOneThroughFour() {
        for (int maximumAttempts = 1; maximumAttempts <= 4; maximumAttempts++) {
            int expectedMaximumAttempts = maximumAttempts;
            this.applicationContextRunner
                    .withPropertyValues("gregor.webhook.maximum-attempts=" + expectedMaximumAttempts)
                    .run(assertableApplicationContext -> {
                        Assertions.assertThat(assertableApplicationContext.getStartupFailure()).isNull();
                        Assertions.assertThat(assertableApplicationContext.getBean(WebhookProperties.class).maximumAttempts()).isEqualTo(expectedMaximumAttempts);
                    });
        }
    }

    @Test
    void rejectsMaximumAttemptsOutsideOneThroughFour() {
        for (int maximumAttempts : List.of(0, 5)) {
            this.applicationContextRunner
                    .withPropertyValues("gregor.webhook.maximum-attempts=" + maximumAttempts)
                    .run(assertableApplicationContext -> Assertions.assertThat(assertableApplicationContext.getStartupFailure()).hasRootCauseInstanceOf(BindValidationException.class));
        }
    }

    @Test
    void acceptsRetentionDurationsAboveTheirExclusiveMinimums() {
        this.applicationContextRunner
                .withPropertyValues("gregor.kafka.topics.retry-retention=3600001ms", "gregor.kafka.topics.dead-letter-retention=1ms")
                .run(assertableApplicationContext -> {
                    Assertions.assertThat(assertableApplicationContext.getStartupFailure()).isNull();
                    KafkaTopicProperties kafkaTopicProperties = assertableApplicationContext.getBean(KafkaTopicProperties.class);
                    Assertions.assertThat(kafkaTopicProperties.retryRetention()).isEqualTo(Duration.ofMillis(3_600_001L));
                    Assertions.assertThat(kafkaTopicProperties.deadLetterRetention()).isEqualTo(Duration.ofMillis(1L));
                });
    }

    @Test
    void rejectsRetryRetentionAtOrBelowOneHour() {
        for (String retryRetention : List.of("-1ms", "0ms", "1h")) {
            this.applicationContextRunner
                    .withPropertyValues("gregor.kafka.topics.retry-retention=" + retryRetention)
                    .run(assertableApplicationContext -> Assertions.assertThat(assertableApplicationContext.getStartupFailure()).hasRootCauseInstanceOf(BindValidationException.class));
        }
    }

    @Test
    void rejectsNonPositiveDeadLetterRetention() {
        for (String deadLetterRetention : List.of("-1ms", "0ms")) {
            this.applicationContextRunner
                    .withPropertyValues("gregor.kafka.topics.dead-letter-retention=" + deadLetterRetention)
                    .run(assertableApplicationContext -> Assertions.assertThat(assertableApplicationContext.getStartupFailure()).hasRootCauseInstanceOf(BindValidationException.class));
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties({WebhookProperties.class, KafkaConsumerProperties.class, KafkaTopicProperties.class})
    private static class PropertiesConfiguration {
    }
}
