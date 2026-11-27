package gregor.config;

import gregor.config.properties.DeadLetterCleanupProperties;
import gregor.config.properties.KafkaConsumerProperties;
import gregor.config.properties.KafkaTopicProperties;
import gregor.config.properties.WebhookProperties;
import org.apache.commons.lang3.Strings;
import org.assertj.core.api.Assertions;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
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
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class ConfigurationPropertiesTests {

    private final ApplicationContextRunner applicationContextRunner = new ApplicationContextRunner()
            .withInitializer(new ConfigDataApplicationContextInitializer())
            .withConfiguration(AutoConfigurations.of(ConfigurationPropertiesAutoConfiguration.class, ValidationAutoConfiguration.class))
            .withUserConfiguration(PropertiesConfiguration.class, TimeConfiguration.class, DeadLetterBatchSizeBindingConfiguration.class)
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
            DeadLetterCleanupProperties deadLetterCleanupProperties = assertableApplicationContext.getBean(DeadLetterCleanupProperties.class);
            Assertions.assertThat(deadLetterCleanupProperties.cleanupInterval()).isEqualTo(Duration.ofMinutes(1));
            Assertions.assertThat(deadLetterCleanupProperties.batchSize()).isEqualTo(100);
            Assertions.assertThat(assertableApplicationContext.getBean(Clock.class).getZone().normalized()).isEqualTo(ZoneOffset.UTC);
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
                .withInitializer(assertableApplicationContext -> addEnvironmentOverrides(
                        assertableApplicationContext.getEnvironment(),
                        Map.of(
                                "WEBHOOK_MAXIMUM_ATTEMPTS", "1",
                                "KAFKA_MAX_POLL_INTERVAL_MS", "4200000",
                                "KAFKA_RETRY_TOPIC_RETENTION", "2h",
                                "KAFKA_DEAD_LETTER_TOPIC_RETENTION", "3d",
                                "DEAD_LETTER_CLEANUP_INTERVAL", "PT2S",
                                "DEAD_LETTER_CLEANUP_BATCH_SIZE", "42"
                        )
                ))
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
                    DeadLetterCleanupProperties deadLetterCleanupProperties = assertableApplicationContext.getBean(DeadLetterCleanupProperties.class);
                    Assertions.assertThat(deadLetterCleanupProperties.cleanupInterval()).isEqualTo(Duration.ofSeconds(2));
                    Assertions.assertThat(deadLetterCleanupProperties.batchSize()).isEqualTo(42);
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
                .withPropertyValues("gregor.kafka.topics.retry-retention=3600001ms", "gregor.kafka.topics.dead-letter-retention=PT0.000000001S")
                .run(assertableApplicationContext -> {
                    Assertions.assertThat(assertableApplicationContext.getStartupFailure()).isNull();
                    KafkaTopicProperties kafkaTopicProperties = assertableApplicationContext.getBean(KafkaTopicProperties.class);
                    Assertions.assertThat(kafkaTopicProperties.retryRetention()).isEqualTo(Duration.ofMillis(3_600_001L));
                    Assertions.assertThat(kafkaTopicProperties.deadLetterRetention()).isEqualTo(Duration.ofNanos(1L));
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

    @ParameterizedTest
    @ValueSource(strings = {"-1ns", "0ns"})
    void rejectsNonPositiveDeadLetterRetention(String deadLetterRetention) {
        this.withEnvironmentOverride("KAFKA_DEAD_LETTER_TOPIC_RETENTION", deadLetterRetention)
                .run(assertableApplicationContext -> Assertions.assertThat(assertableApplicationContext.getStartupFailure()).hasRootCauseInstanceOf(BindValidationException.class));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "nonsense", "1fortnight"})
    void rejectsEmptyOrMalformedDeadLetterRetentionOverrides(String deadLetterRetention) {
        this.withEnvironmentOverride("KAFKA_DEAD_LETTER_TOPIC_RETENTION", deadLetterRetention)
                .run(assertableApplicationContext -> Assertions.assertThat(assertableApplicationContext.getStartupFailure()).hasStackTraceContaining("gregor.kafka.topics"));
    }

    @Test
    void rejectsMissingEffectiveDeadLetterRetention() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ConfigurationPropertiesAutoConfiguration.class, ValidationAutoConfiguration.class))
                .withUserConfiguration(KafkaTopicPropertiesConfiguration.class)
                .withPropertyValues("gregor.kafka.topics.default-partitions=12", "gregor.kafka.topics.replication-factor=1", "gregor.kafka.topics.retry-retention=7d")
                .run(assertableApplicationContext -> Assertions.assertThat(assertableApplicationContext.getStartupFailure()).hasRootCauseInstanceOf(BindValidationException.class).hasStackTraceContaining("deadLetterRetention"));
    }

    @Test
    void acceptsBothSupportedCleanupLimits() {
        this.assertCleanupSettings("1ms", "1", Duration.ofMillis(1), 1);
        this.assertCleanupSettings("4611686018427387902ns", "1000", Duration.ofNanos(4_611_686_018_427_387_902L), 1000);
        this.assertCleanupSettings("1500", "42", Duration.ofMillis(1500), 42);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "nonsense", "1fortnight", "0ms", "-1ms", "999999ns", "4611686018427387903ns", "PT30000000000000H"})
    void rejectsInvalidCleanupIntervals(String interval) {
        this.withEnvironmentOverride("DEAD_LETTER_CLEANUP_INTERVAL", interval).run(assertableApplicationContext -> Assertions.assertThat(assertableApplicationContext.getStartupFailure()).hasStackTraceContaining("gregor.dead-letter"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            " ",
            "1.5",
            "2.0",
            "1e2",
            "+1",
            "-1",
            "0",
            "1001",
            "2147483648"
    })
    void rejectsInvalidCleanupBatchSizes(String batchSize) {
        this.withEnvironmentOverride("DEAD_LETTER_CLEANUP_BATCH_SIZE", batchSize)
                .run(assertableApplicationContext -> Assertions.assertThat(assertableApplicationContext.getStartupFailure()).hasRootCauseInstanceOf(BindValidationException.class));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "1.5",
            "2.0",
            "1e2",
            "0x10",
            "0b10",
            "1:00",
            "1_0",
            "+42",
            "0",
            "1001",
            "!!int '0x10'",
            "!!binary Kg==",
            "!!binary 1000",
            "!!float 42",
            "[42]",
            "[]",
            "{value: 42}",
            "{'': 42}",
            "{'!': 42}",
            "{}"
    })
    void rejectsInvalidCleanupBatchSizesFromYaml(String batchSize, @TempDir Path directory) throws IOException {
        this.withYamlSettings(directory, "gregor:\n    dead-letter:\n        cleanup-batch-size: " + batchSize)
                .run(assertableApplicationContext -> Assertions.assertThat(assertableApplicationContext.getStartupFailure()).hasRootCauseInstanceOf(BindValidationException.class).hasStackTraceContaining("cleanupBatchSize"));
    }

    @ParameterizedTest
    @CsvSource({
            "1,1",
            "100,100",
            "1000,1000",
            "0010,10",
            "\"100\",100"
    })
    void bindsDecimalCleanupBatchSizesFromYaml(String batchSize, int expectedBatchSize, @TempDir Path directory) throws IOException {
        this.withYamlSettings(directory, "gregor.dead-letter.cleanupBatchSize: " + batchSize)
                .run(assertableApplicationContext -> Assertions.assertThat(assertableApplicationContext.getBean(DeadLetterCleanupProperties.class).batchSize()).isEqualTo(expectedBatchSize));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "DEAD_LETTER_CLEANUP_BATCH_SIZE: 0x10",
            "defaults.batch-size: 0x10",
            "defaults.batch-size: !!binary Kg==",
            "batch-selector: 0010\nbatch.8: 0x10\ndefaults.batch-size: ${batch.${batch-selector}:42}"
    })
    void rejectsNonDecimalCleanupBatchSizeFromAYamlPlaceholderValue(String yaml, @TempDir Path directory) throws IOException {
        this.withYamlSettings(directory, "gregor.dead-letter.cleanup-batch-size: ${defaults.batch-size:${DEAD_LETTER_CLEANUP_BATCH_SIZE:100}}\n---\n" + yaml)
                .run(assertableApplicationContext -> Assertions.assertThat(assertableApplicationContext.getStartupFailure()).hasRootCauseInstanceOf(BindValidationException.class).hasStackTraceContaining("cleanupBatchSize"));
    }

    @Test
    void bindsDecimalYamlPlaceholderWithoutChangingTheReferencedProperty(@TempDir Path directory) throws IOException {
        this.withYamlSettings(
                        directory,
                        """
                        defaults.batchSize: 0010
                        batch-alias: ${defaults.batch-size}
                        gregor.dead-letter.cleanup-batch-size: ${batch-alias}
                        """
                )
                .run(assertableApplicationContext -> {
                    Assertions.assertThat(assertableApplicationContext.getBean(DeadLetterCleanupProperties.class).batchSize()).isEqualTo(10);
                    Assertions.assertThat(assertableApplicationContext.getEnvironment().getProperty("defaults.batchSize", Integer.class)).isEqualTo(8);
                });
    }

    @ParameterizedTest
    @ValueSource(strings = {"${batch.${batch-selector}}", "${missing:${batch.${batch-selector}}}"})
    void resolvesNestedYamlPlaceholderKeysWithBootValues(String batchSize, @TempDir Path directory) throws IOException {
        this.withYamlSettings(
                        directory,
                        """
                        batch-selector: 0010
                        batch.8: 0042
                        batch.0010: 99
                        gregor.dead-letter.cleanup-batch-size: %s
                        """.formatted(batchSize)
                )
                .run(assertableApplicationContext -> {
                    Assertions.assertThat(assertableApplicationContext.getBean(DeadLetterCleanupProperties.class).batchSize()).isEqualTo(42);
                    Assertions.assertThat(assertableApplicationContext.getEnvironment().getProperty("gregor.dead-letter.cleanup-batch-size", Integer.class)).isEqualTo(34);
                });
    }

    @ParameterizedTest
    @ValueSource(strings = {"0x10", "[42]", "{value: 42}"})
    void allowsAHigherPrecedenceOverrideOfAnInvalidYamlBatchSize(String batchSize, @TempDir Path directory) throws IOException {
        this.withYamlSettings(directory, "gregor.dead-letter.cleanupBatchSize: " + batchSize)
                .withPropertyValues("gregor.dead-letter.cleanup-batch-size=42")
                .run(assertableApplicationContext -> Assertions.assertThat(assertableApplicationContext.getBean(DeadLetterCleanupProperties.class).batchSize()).isEqualTo(42));
    }

    @Test
    void givesPropertiesFilesPrecedenceOverSiblingYamlFiles(@TempDir Path directory) throws IOException {
        Files.writeString(directory.resolve("application.properties"), "gregor.dead-letter.cleanup-batch-size=42\nexample.setting=properties\n");
        Files.writeString(directory.resolve("application.yaml"), "gregor.dead-letter.cleanup-batch-size: 0x10\nexample.setting: yaml\n");
        this.applicationContextRunner
                .withPropertyValues("spring.config.additional-location=" + directory.toUri())
                .run(assertableApplicationContext -> {
                    Assertions.assertThat(assertableApplicationContext.getBean(DeadLetterCleanupProperties.class).batchSize()).isEqualTo(42);
                    Assertions.assertThat(assertableApplicationContext.getEnvironment().getProperty("example.setting")).isEqualTo("properties");
                });
    }

    @Test
    void bindsOnlyValuesFromTheOriginalYamlSnapshot() {
        Resource resource = new AbstractResource() {
            private boolean opened;

            @Override
            @NullMarked
            public String getDescription() {
                return "replaceable cleanup YAML";
            }

            @Override
            @NullMarked
            public InputStream getInputStream() {
                String batchSize = this.opened ? "1000" : "0010";
                this.opened = true;
                return new ByteArrayInputStream(("gregor.dead-letter.cleanup-batch-size: " + batchSize).getBytes(StandardCharsets.UTF_8));
            }
        };
        this.applicationContextRunner
                .withInitializer(assertableApplicationContext -> {
                    try {
                        new DeadLetterBatchSizeYamlPropertySourceLoader().load("cleanup", resource).forEach(assertableApplicationContext.getEnvironment().getPropertySources()::addFirst);
                    } catch (IOException ioException) {
                        throw new UncheckedIOException(ioException);
                    }
                })
                .run(assertableApplicationContext -> Assertions.assertThat(assertableApplicationContext.getBean(DeadLetterCleanupProperties.class).batchSize()).isEqualTo(10));
    }

    @ParameterizedTest
    @ValueSource(strings = {"gregor.dead-letter.cleanup-interval=1m", "gregor.dead-letter.cleanup-batch-size=100"})
    void rejectsEitherMissingEffectiveCleanupSetting(String otherSetting) {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ConfigurationPropertiesAutoConfiguration.class, ValidationAutoConfiguration.class))
                .withUserConfiguration(CleanupPropertiesConfiguration.class)
                .withPropertyValues(otherSetting)
                .run(assertableApplicationContext -> Assertions.assertThat(assertableApplicationContext.getStartupFailure()).hasRootCauseInstanceOf(BindValidationException.class));
    }

    private ApplicationContextRunner withYamlSettings(Path directory, String yaml) throws IOException {
        Path configPath = Files.writeString(directory.resolve("cleanup.yaml"), yaml);
        return this.applicationContextRunner.withPropertyValues("spring.config.additional-location=" + configPath.toUri());
    }

    private ApplicationContextRunner withEnvironmentOverride(String name, String value) {
        return this.applicationContextRunner.withInitializer(assertableApplicationContext -> addEnvironmentOverrides(assertableApplicationContext.getEnvironment(), Map.of(name, value)));
    }

    private static void addEnvironmentOverrides(ConfigurableEnvironment configurableEnvironment, Map<String, Object> environmentOverrides) {
        configurableEnvironment.getPropertySources().addFirst(new SystemEnvironmentPropertySource("test-systemEnvironment", environmentOverrides));
    }

    private void assertCleanupSettings(String interval, String batchSize, Duration expectedInterval, int expectedBatchSize) {
        this.applicationContextRunner
                .withPropertyValues("gregor.dead-letter.cleanup-interval=" + interval, "gregor.dead-letter.cleanup-batch-size=" + batchSize)
                .run(assertableApplicationContext -> {
                    Assertions.assertThat(assertableApplicationContext.getStartupFailure()).isNull();
                    DeadLetterCleanupProperties deadLetterCleanupProperties = assertableApplicationContext.getBean(DeadLetterCleanupProperties.class);
                    Assertions.assertThat(deadLetterCleanupProperties.cleanupInterval()).isEqualTo(expectedInterval);
                    Assertions.assertThat(deadLetterCleanupProperties.batchSize()).isEqualTo(expectedBatchSize);
                });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(DeadLetterCleanupProperties.class)
    private static class CleanupPropertiesConfiguration {
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(KafkaTopicProperties.class)
    private static class KafkaTopicPropertiesConfiguration {
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties({WebhookProperties.class, KafkaConsumerProperties.class, KafkaTopicProperties.class, DeadLetterCleanupProperties.class})
    private static class PropertiesConfiguration {
    }
}
