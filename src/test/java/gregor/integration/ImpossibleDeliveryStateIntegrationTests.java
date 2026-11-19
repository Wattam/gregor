package gregor.integration;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import gregor.avro.WebhookEvent;
import gregor.route.WebhookRoute;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "gregor.webhook.maximum-attempts=4",
        "camel.component.kafka.max-poll-interval-ms=4500000"
})
class ImpossibleDeliveryStateIntegrationTests {

    private static final int MAXIMUM_ATTEMPTS = 4;
    private static final String TENANT_ID = "00000000-0000-0000-0000-000000000001";
    private static final String ENDPOINT_ID = "00000000-0000-0000-0000-000000000002";
    private static final String PAYLOAD_MARKER = "impossible-state-payload-marker";
    private static final String SIGNING_SECRET = "whsec_" + Base64.getEncoder().encodeToString(
            "01234567890123456789012345678901".getBytes(StandardCharsets.UTF_8)
    );
    private static final String[] DOWNSTREAM_TOPICS = {
            WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC, WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC,
            WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC, WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC,
            WebhookRoute.WEBHOOK_EVENT_DEAD_LETTER_TOPIC
    };
    private static final TestReceiver RECEIVER = TestReceiver.start();

    @Autowired
    private ProducerTemplate producerTemplate;
    @Autowired
    private JdbcTemplate jdbcTemplate;

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
        registry.add("camel.component.kafka.consumers-count", () -> "1");
        registry.add("camel.component.kafka.auto-commit-enable", () -> "false");
        registry.add("camel.component.kafka.allow-manual-commit", () -> "true");
        registry.add("camel.component.kafka.break-on-first-error", () -> "true");
        registry.add("camel.component.kafka.auto-offset-reset", () -> "earliest");
        registry.add("camel.component.kafka.max-poll-records", () -> "1");
        registry.add("camel.main.auto-startup", () -> "true");
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
        registry.add("gregor.kafka.listener-concurrency", () -> "1");
        registry.add("gregor.webhook.tenant-id", () -> TENANT_ID);
        registry.add("gregor.webhook.endpoint-id", () -> ENDPOINT_ID);
        registry.add("gregor.webhook.receiver-url", RECEIVER::uri);
        registry.add("gregor.webhook.signing-secret", () -> SIGNING_SECRET);
        registry.add("gregor.webhook.connection-timeout", () -> "2s");
        registry.add("gregor.webhook.response-timeout", () -> "5s");
        registry.add("gregor.webhook.receiver-timestamp-tolerance", () -> "5m");
    }

    @AfterAll
    static void stopReceiver() {
        RECEIVER.close();
    }

    @Test
    void discardsInvalidSourceEnvelopesWithoutDeliveryAndCommitsTheirOffsets() throws Exception {
        List<WebhookEvent> impossibleEvents = List.of(
                event("exhausted-attempt-count.test", MAXIMUM_ATTEMPTS, 0, 0, null),
                event("negative-attempt-count.test", -1, 0, 0, null)
        );
        TopicPartition partition = new TopicPartition(WebhookRoute.WEBHOOK_EVENT_TOPIC, 0);
        try (AdminClient adminClient = KafkaTestRecords.adminClient()) {
            Map<TopicPartition, Long> before = KafkaTestRecords.endOffsets(adminClient, DOWNSTREAM_TOPICS);
            for (WebhookEvent impossibleEvent : impossibleEvents) {
                this.assertDiscarded(adminClient, partition, impossibleEvent);
            }
            Assertions.assertThat(KafkaTestRecords.endOffsets(adminClient, DOWNSTREAM_TOPICS)).isEqualTo(before);
        }
    }

    @Test
    void discardsMalformedRetryEnvelopesWithoutDeliveryOrDownstreamPublicationAndCommitsOffsets() throws Exception {
        Instant now = Instant.now();
        List<RetryRecord> malformedRecords = List.of(
                new RetryRecord(
                        WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC,
                        event("retry-exhausted.test", MAXIMUM_ATTEMPTS, 0, 0, now)
                ),
                new RetryRecord(
                        WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC,
                        event("retry-null-eligibility.test", 0, 0, 0, null)
                ),
                new RetryRecord(
                        WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC,
                        event("retry-distant-eligibility.test", 0, 0, 0, now.plus(Duration.ofHours(2)))
                )
        );
        try (AdminClient admin = KafkaTestRecords.adminClient()) {
            Map<TopicPartition, Long> expected = new HashMap<>(KafkaTestRecords.endOffsets(admin, DOWNSTREAM_TOPICS));
            for (RetryRecord malformedRecord : malformedRecords) {
                TopicPartition partition = new TopicPartition(malformedRecord.topic(), 0);
                this.assertDiscarded(admin, partition, malformedRecord.event());
                expected.compute(partition, (_, offset) -> offset + 1);
            }
            Assertions.assertThat(KafkaTestRecords.endOffsets(admin, DOWNSTREAM_TOPICS)).isEqualTo(expected);
        }
    }

    @Test
    void deliversAnEnvelopeWithAFutureNextEligibleAtWithoutDelayingOrSkippingIt() throws Exception {
        WebhookEvent eligibleLaterEvent = event(
                "next-eligible-at.test",
                0,
                0,
                0,
                Instant.now().plus(Duration.ofHours(2))
        );
        Instant sendStartedAt = Instant.now();

        this.producerTemplate.sendBodyAndHeaders(
                "kafka:" + WebhookRoute.WEBHOOK_EVENT_TOPIC,
                eligibleLaterEvent,
                Map.of(KafkaConstants.KEY, ENDPOINT_ID, KafkaConstants.PARTITION_KEY, 0)
        );

        RECEIVER.awaitRequest(eligibleLaterEvent.getEventId());
        Assertions.assertThat(Duration.between(sendStartedAt, Instant.now())).isLessThan(Duration.ofSeconds(5));
        Map<String, Object> row = this.awaitAttemptRow(eligibleLaterEvent.getEventId());
        Assertions.assertThat(row)
                .containsEntry("attempt_number", 1)
                .containsEntry("outcome", "SUCCEEDED")
                .containsEntry("http_status", 204)
                .containsEntry("retry_count", 0)
                .containsEntry("replay_count", 0)
                .containsEntry("dead_lettered", false);
    }

    @Test
    void deliversAnEnvelopeWhoseRetryCountExceedsItsDeliveryAttemptCountWithTheEnvelopeCounters() throws Exception {
        WebhookEvent aheadOfAttemptsEvent = event("retry-count-ahead.test", 0, 3, 2, null);

        this.producerTemplate.sendBodyAndHeaders(
                "kafka:" + WebhookRoute.WEBHOOK_EVENT_TOPIC,
                aheadOfAttemptsEvent,
                Map.of(KafkaConstants.KEY, ENDPOINT_ID, KafkaConstants.PARTITION_KEY, 0)
        );

        RECEIVER.awaitRequest(aheadOfAttemptsEvent.getEventId());
        Map<String, Object> row = this.awaitAttemptRow(aheadOfAttemptsEvent.getEventId());
        Assertions.assertThat(row)
                .containsEntry("attempt_number", 1)
                .containsEntry("outcome", "SUCCEEDED")
                .containsEntry("retry_count", 3)
                .containsEntry("replay_count", 2)
                .containsEntry("dead_lettered", false);
    }

    private void assertDiscarded(AdminClient admin, TopicPartition partition, WebhookEvent event) throws Exception {
        long inputOffset = admin.listOffsets(Map.of(partition, OffsetSpec.latest()))
                .partitionResult(partition).get(5, TimeUnit.SECONDS).offset();
        this.producerTemplate.sendBodyAndHeaders(
                "kafka:" + partition.topic(), event,
                Map.of(KafkaConstants.KEY, ENDPOINT_ID, KafkaConstants.PARTITION_KEY, partition.partition())
        );
        KafkaTestRecords.awaitCommit(admin, partition, inputOffset);
        Assertions.assertThat(RECEIVER.count(event.getEventId())).isZero();
        Assertions.assertThat(this.attemptRowCount(event.getEventId())).isZero();
    }

    private Map<String, Object> awaitAttemptRow(String eventId) throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(15);
        while (Instant.now().isBefore(deadline)) {
            if (this.attemptRowCount(eventId) == 1) {
                return this.jdbcTemplate.queryForMap(
                        """
                        SELECT attempt_number, outcome, http_status, retry_count, replay_count, dead_lettered
                        FROM delivery_attempt
                        WHERE event_id = ?::uuid
                        """,
                        eventId
                );
            }
            Thread.sleep(50L);
        }
        throw new AssertionError("Expected one attempt row for event " + eventId);
    }

    private int attemptRowCount(String eventId) {
        Integer count = this.jdbcTemplate.queryForObject(
                "SELECT count(*) FROM delivery_attempt WHERE event_id = ?::uuid",
                Integer.class,
                eventId
        );
        return count == null ? 0 : count;
    }

    private static WebhookEvent event(
            String eventType,
            int deliveryAttemptCount,
            int retryCount,
            int replayCount,
            Instant nextEligibleAt
    ) {
        return new WebhookEvent(
                UUID.randomUUID().toString(),
                eventType,
                TENANT_ID,
                ENDPOINT_ID,
                Instant.now(),
                ByteBuffer.wrap(("{\"private\":\"" + PAYLOAD_MARKER + "\"}").getBytes(StandardCharsets.UTF_8)),
                retryCount,
                nextEligibleAt,
                deliveryAttemptCount,
                replayCount
        );
    }

    private record RetryRecord(String topic, WebhookEvent event) {
    }

    private record CapturedRequest(String webhookId) {
    }

    private static final class TestReceiver implements AutoCloseable {

        private final HttpServer server;
        private final ExecutorService executor;
        private final ConcurrentLinkedQueue<CapturedRequest> requests = new ConcurrentLinkedQueue<>();

        private TestReceiver(HttpServer server, ExecutorService executor) {
            this.server = server;
            this.executor = executor;
        }

        static TestReceiver start() {
            try {
                HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
                ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
                TestReceiver receiver = new TestReceiver(server, executor);
                server.createContext("/webhook", receiver::handle);
                server.setExecutor(executor);
                server.start();
                return receiver;
            } catch (IOException exception) {
                throw new IllegalStateException("Could not start impossible-state receiver", exception);
            }
        }

        String uri() {
            return "http://localhost:" + this.server.getAddress().getPort() + "/webhook";
        }

        int count(String eventId) {
            return (int) this.requests.stream()
                    .filter(request -> eventId.equals(request.webhookId()))
                    .count();
        }

        void awaitRequest(String eventId) throws InterruptedException {
            Instant deadline = Instant.now().plusSeconds(15);
            while (Instant.now().isBefore(deadline)) {
                if (this.count(eventId) >= 1) {
                    return;
                }
                Thread.sleep(25L);
            }
            throw new AssertionError("Expected a request for event " + eventId);
        }

        private void handle(HttpExchange exchange) throws IOException {
            exchange.getRequestBody().readAllBytes();
            this.requests.add(new CapturedRequest(exchange.getRequestHeaders().getFirst("webhook-id")));
            try (exchange) {
                exchange.sendResponseHeaders(204, -1L);
            }
        }

        @Override
        public void close() {
            this.server.stop(0);
            this.executor.shutdownNow();
        }
    }
}
