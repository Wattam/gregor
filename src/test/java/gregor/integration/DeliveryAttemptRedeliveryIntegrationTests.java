package gregor.integration;

import gregor.avro.DeliveryAttempt;
import gregor.avro.DeliveryOutcome;
import gregor.route.WebhookRoute;
import gregor.service.WebhookService;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class DeliveryAttemptRedeliveryIntegrationTests {

    private static final String SIGNING_SECRET = "whsec_" + Base64.getEncoder().encodeToString("01234567890123456789012345678901".getBytes(StandardCharsets.UTF_8));

    @Autowired
    private CamelContext camelContext;
    @Autowired
    private ProducerTemplate producerTemplate;
    @MockitoBean
    private WebhookService webhookService;

    @DynamicPropertySource
    static void configureInfrastructure(DynamicPropertyRegistry dynamicPropertyRegistry) {
        String postgresHost = System.getenv().getOrDefault("POSTGRES_HOST", "localhost");
        String postgresPort = System.getenv().getOrDefault("POSTGRES_PORT", "5432");
        String postgresDatabase = System.getenv().getOrDefault("POSTGRES_DB", "gregor");
        String kafkaPort = System.getenv().getOrDefault("KAFKA_PORT", "9092");
        String registryPort = System.getenv().getOrDefault("SCHEMA_REGISTRY_PORT", "8081");

        dynamicPropertyRegistry.add("spring.datasource.url", () -> "jdbc:postgresql://" + postgresHost + ":" + postgresPort + "/" + postgresDatabase);
        dynamicPropertyRegistry.add("spring.datasource.username", () -> System.getenv().getOrDefault("POSTGRES_USER", "gregor"));
        dynamicPropertyRegistry.add("spring.datasource.password", () -> System.getenv().getOrDefault("POSTGRES_PASSWORD", "gregor"));
        dynamicPropertyRegistry.add("spring.jpa.hibernate.ddl-auto", () -> "update");
        dynamicPropertyRegistry.add("spring.kafka.bootstrap-servers", () -> "localhost:" + kafkaPort);
        dynamicPropertyRegistry.add("spring.kafka.admin.auto-create", () -> "true");
        dynamicPropertyRegistry.add("camel.component.kafka.brokers", () -> "localhost:" + kafkaPort);
        dynamicPropertyRegistry.add("camel.component.kafka.auto-commit-enable", () -> "false");
        dynamicPropertyRegistry.add("camel.component.kafka.allow-manual-commit", () -> "true");
        dynamicPropertyRegistry.add("camel.component.kafka.break-on-first-error", () -> "true");
        dynamicPropertyRegistry.add("camel.component.kafka.auto-offset-reset", () -> "earliest");
        dynamicPropertyRegistry.add("camel.component.kafka.max-poll-records", () -> "1");
        dynamicPropertyRegistry.add("camel.component.kafka.request-required-acks", () -> "all");
        dynamicPropertyRegistry.add("camel.component.kafka.key-serializer", () -> "org.apache.kafka.common.serialization.StringSerializer");
        dynamicPropertyRegistry.add("camel.component.kafka.value-serializer", () -> "io.apicurio.registry.serde.avro.AvroKafkaSerializer");
        dynamicPropertyRegistry.add("camel.component.kafka.key-deserializer", () -> "org.apache.kafka.common.serialization.StringDeserializer");
        dynamicPropertyRegistry.add("camel.component.kafka.value-deserializer", () -> "io.apicurio.registry.serde.avro.AvroKafkaDeserializer");
        dynamicPropertyRegistry.add("camel.component.kafka.additional-properties[apicurio.registry.url]", () -> "http://localhost:" + registryPort + "/apis/registry/v3");
        dynamicPropertyRegistry.add("camel.component.kafka.additional-properties[apicurio.registry.auto-register]", () -> "true");
        dynamicPropertyRegistry.add("camel.component.kafka.additional-properties[apicurio.registry.use-specific-avro-reader]", () -> "true");
        dynamicPropertyRegistry.add("gregor.webhook.tenant-id", () -> "00000000-0000-0000-0000-000000000001");
        dynamicPropertyRegistry.add("gregor.webhook.endpoint-id", () -> "00000000-0000-0000-0000-000000000002");
        dynamicPropertyRegistry.add("gregor.webhook.receiver-url", () -> "http://localhost:1/not-used");
        dynamicPropertyRegistry.add("gregor.webhook.signing-secret", () -> SIGNING_SECRET);
        dynamicPropertyRegistry.add("gregor.webhook.connection-timeout", () -> "2s");
        dynamicPropertyRegistry.add("gregor.webhook.response-timeout", () -> "5s");
        dynamicPropertyRegistry.add("gregor.webhook.receiver-timestamp-tolerance", () -> "5m");
    }

    @Test
    void leavesTheAttemptOffsetUncommittedAndRedeliversAfterPersistenceFails() throws Exception {
        String eventId = UUID.randomUUID().toString();
        DeliveryAttempt deliveryAttempt = new DeliveryAttempt(
                UUID.randomUUID().toString(),
                eventId,
                "00000000-0000-0000-0000-000000000001",
                "00000000-0000-0000-0000-000000000002",
                "attempt-redelivery.test",
                1,
                DeliveryOutcome.SUCCEEDED,
                204,
                1L,
                Instant.now(),
                ByteBuffer.wrap("{}".getBytes(StandardCharsets.UTF_8)),
                0,
                0,
                false
        );
        CountDownLatch redeliveredCountDownLatch = new CountDownLatch(1);
        CountDownLatch persistenceAllowedCountDownLatch = new CountDownLatch(1);
        AtomicInteger persistenceCallCounter = new AtomicInteger();
        Mockito.doAnswer(_ -> {
                    if (persistenceCallCounter.incrementAndGet() == 1) {
                        throw new IllegalStateException("database unavailable");
                    }
                    redeliveredCountDownLatch.countDown();
                    if (!persistenceAllowedCountDownLatch.await(30, TimeUnit.SECONDS)) {
                        throw new AssertionError("Persistence was not released");
                    }
                    return null;
                })
                .when(this.webhookService)
                .persistDeliveryAttempt(Mockito.argThat(candidateDeliveryAttempt -> Objects.equals(eventId, candidateDeliveryAttempt.getEventId())));

        TopicPartition topicPartition = new TopicPartition(WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC, 0);
        String route = "kafka:" + topicPartition.topic() + "?groupId=" + topicPartition.topic();
        this.camelContext.getRouteController().startRoute(route);
        try (AdminClient adminClient = this.adminClient()) {
            long offset = adminClient.listOffsets(Map.of(topicPartition, OffsetSpec.latest())).partitionResult(topicPartition).get(5, TimeUnit.SECONDS).offset();
            try {
                this.producerTemplate.sendBodyAndHeaders("kafka:" + topicPartition.topic(), deliveryAttempt, Map.of(KafkaConstants.KEY, eventId, KafkaConstants.PARTITION_KEY, topicPartition.partition()));
                Assertions.assertThat(redeliveredCountDownLatch.await(15, TimeUnit.SECONDS)).isTrue();
                Assertions.assertThat(committedOffset(adminClient, topicPartition)).isLessThanOrEqualTo(offset);
            } finally {
                persistenceAllowedCountDownLatch.countDown();
            }

            awaitCommittedOffset(adminClient, topicPartition, offset + 1);
        }
    }

    private static long committedOffset(AdminClient adminClient, TopicPartition topicPartition) throws Exception {
        OffsetAndMetadata committedOffsetAndMetadata = adminClient.listConsumerGroupOffsets(WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS).get(topicPartition);
        return Objects.isNull(committedOffsetAndMetadata) ? -1 : committedOffsetAndMetadata.offset();
    }

    private static void awaitCommittedOffset(AdminClient adminClient, TopicPartition topicPartition, long offset) throws Exception {
        Instant deadline = Instant.now().plusSeconds(15);
        while (Instant.now().isBefore(deadline)) {
            if (committedOffset(adminClient, topicPartition) >= offset) {
                return;
            }
            Thread.sleep(50L);
        }
        Assertions.fail("Attempt offset was not committed after successful redelivery");
    }

    private AdminClient adminClient() {
        return AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + System.getenv().getOrDefault("KAFKA_PORT", "9092")));
    }
}
