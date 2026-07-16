package gregor.integration;

import gregor.avro.DeliveryAttempt;
import gregor.avro.DeliveryOutcome;
import gregor.route.WebhookRoute;
import gregor.service.WebhookService;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class DeliveryAttemptRedeliveryIntegrationTests {

    private static final String SIGNING_SECRET = "whsec_" + Base64.getEncoder().encodeToString(
            "01234567890123456789012345678901".getBytes(StandardCharsets.UTF_8)
    );

    @Autowired
    private ProducerTemplate producerTemplate;

    @MockitoBean
    private WebhookService webhookService;

    @DynamicPropertySource
    static void configureInfrastructure(DynamicPropertyRegistry registry) {
        String postgresHost = System.getenv().getOrDefault("POSTGRES_HOST", "localhost");
        String postgresPort = System.getenv().getOrDefault("POSTGRES_PORT", "5432");
        String postgresDatabase = System.getenv().getOrDefault("POSTGRES_DB", "gregor");
        String kafkaPort = System.getenv().getOrDefault("KAFKA_PORT", "9092");
        String registryPort = System.getenv().getOrDefault("SCHEMA_REGISTRY_PORT", "8081");

        registry.add(
                "spring.datasource.url",
                () -> "jdbc:postgresql://" + postgresHost + ":" + postgresPort + "/" + postgresDatabase
        );
        registry.add("spring.datasource.username", () -> System.getenv().getOrDefault("POSTGRES_USER", "gregor"));
        registry.add(
                "spring.datasource.password",
                () -> System.getenv().getOrDefault("POSTGRES_PASSWORD", "gregor")
        );
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "update");
        registry.add("spring.kafka.bootstrap-servers", () -> "localhost:" + kafkaPort);
        registry.add("spring.kafka.admin.auto-create", () -> "true");
        registry.add("camel.component.kafka.brokers", () -> "localhost:" + kafkaPort);
        registry.add("camel.component.kafka.consumers-count", () -> "12");
        registry.add("camel.component.kafka.auto-commit-enable", () -> "false");
        registry.add("camel.component.kafka.allow-manual-commit", () -> "true");
        registry.add("camel.component.kafka.break-on-first-error", () -> "true");
        registry.add("camel.component.kafka.auto-offset-reset", () -> "earliest");
        registry.add("camel.component.kafka.max-poll-records", () -> "1");
        registry.add("camel.springboot.auto-startup", () -> "true");
        registry.add("camel.component.kafka.request-required-acks", () -> "all");
        registry.add(
                "camel.component.kafka.key-serializer",
                () -> "org.apache.kafka.common.serialization.StringSerializer"
        );
        registry.add(
                "camel.component.kafka.value-serializer",
                () -> "io.apicurio.registry.serde.avro.AvroKafkaSerializer"
        );
        registry.add(
                "camel.component.kafka.key-deserializer",
                () -> "org.apache.kafka.common.serialization.StringDeserializer"
        );
        registry.add(
                "camel.component.kafka.value-deserializer",
                () -> "io.apicurio.registry.serde.avro.AvroKafkaDeserializer"
        );
        registry.add(
                "camel.component.kafka.additional-properties[apicurio.registry.url]",
                () -> "http://localhost:" + registryPort + "/apis/registry/v3"
        );
        registry.add("camel.component.kafka.additional-properties[apicurio.registry.auto-register]", () -> "true");
        registry.add(
                "camel.component.kafka.additional-properties[apicurio.registry.use-specific-avro-reader]",
                () -> "true"
        );
        registry.add("gregor.kafka.listener-concurrency", () -> "12");
        registry.add("gregor.webhook.tenant-id", () -> "00000000-0000-0000-0000-000000000001");
        registry.add("gregor.webhook.endpoint-id", () -> "00000000-0000-0000-0000-000000000002");
        registry.add("gregor.webhook.receiver-url", () -> "http://localhost:1/not-used");
        registry.add("gregor.webhook.signing-secret", () -> SIGNING_SECRET);
        registry.add("gregor.webhook.connection-timeout", () -> "2s");
        registry.add("gregor.webhook.response-timeout", () -> "5s");
        registry.add("gregor.webhook.receiver-timestamp-tolerance", () -> "5m");
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
        Mockito.doThrow(new IllegalStateException("database unavailable"))
                .doNothing()
                .when(this.webhookService)
                .persistDeliveryAttempt(Mockito.argThat(candidate -> eventId.equals(candidate.getEventId())));

        this.producerTemplate.sendBodyAndHeaders(
                "kafka:" + WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC,
                deliveryAttempt,
                Map.of(KafkaConstants.KEY, eventId, KafkaConstants.PARTITION_KEY, 0)
        );
        TopicPartition partition = new TopicPartition("persist-delivery-attempt", 0);
        long expectedOffset = this.endOffset(partition);

        Mockito.verify(this.webhookService, Mockito.timeout(15_000).atLeast(2)).persistDeliveryAttempt(
                Mockito.argThat(redelivered -> eventId.equals(redelivered.getEventId())
                        && redelivered.getRetryCount() == 0
                        && redelivered.getReplayCount() == 0
                        && !redelivered.getDeadLettered())
        );
        this.awaitCommittedOffset(partition, expectedOffset);
    }

    private long endOffset(TopicPartition partition) throws Exception {
        try (AdminClient adminClient = this.adminClient()) {
            return adminClient.listOffsets(Map.of(partition, OffsetSpec.latest()))
                    .partitionResult(partition)
                    .get(5, TimeUnit.SECONDS)
                    .offset();
        }
    }

    private void awaitCommittedOffset(TopicPartition partition, long expectedOffset) throws Exception {
        try (AdminClient adminClient = this.adminClient()) {
            Instant deadline = Instant.now().plusSeconds(15);
            while (Instant.now().isBefore(deadline)) {
                Map<TopicPartition, OffsetAndMetadata> offsets = adminClient
                        .listConsumerGroupOffsets(WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC)
                        .partitionsToOffsetAndMetadata()
                        .get(5, TimeUnit.SECONDS);
                OffsetAndMetadata committed = offsets.get(partition);
                if (committed != null && committed.offset() >= expectedOffset) {
                    return;
                }
                Thread.sleep(50L);
            }
        }
        Assertions.fail("Attempt offset was not eventually committed after successful redelivery");
    }

    private AdminClient adminClient() {
        return AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:" + System.getenv().getOrDefault("KAFKA_PORT", "9092")
        ));
    }
}
