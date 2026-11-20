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
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {"gregor.webhook.maximum-attempts=4", "camel.component.kafka.max-poll-interval-ms=4500000"})
class ImpossibleDeliveryStateIntegrationTests {

    private static final int MAXIMUM_ATTEMPTS = 4;
    private static final String TENANT_ID = "00000000-0000-0000-0000-000000000001";
    private static final String ENDPOINT_ID = "00000000-0000-0000-0000-000000000002";
    private static final String PAYLOAD_MARKER = "impossible-state-payload-marker";
    private static final String SIGNING_SECRET = "whsec_" + Base64.getEncoder().encodeToString("01234567890123456789012345678901".getBytes(StandardCharsets.UTF_8));
    private static final String[] DOWNSTREAM_TOPICS = {
            WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC,
            WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC,
            WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC,
            WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC,
            WebhookRoute.WEBHOOK_EVENT_DEAD_LETTER_TOPIC
    };
    private static final TestReceiver TEST_RECEIVER = TestReceiver.start();

    @Autowired
    private ProducerTemplate producerTemplate;
    @Autowired
    private JdbcTemplate jdbcTemplate;

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
        dynamicPropertyRegistry.add("camel.component.kafka.consumers-count", () -> "1");
        dynamicPropertyRegistry.add("camel.component.kafka.auto-commit-enable", () -> "false");
        dynamicPropertyRegistry.add("camel.component.kafka.allow-manual-commit", () -> "true");
        dynamicPropertyRegistry.add("camel.component.kafka.break-on-first-error", () -> "true");
        dynamicPropertyRegistry.add("camel.component.kafka.auto-offset-reset", () -> "earliest");
        dynamicPropertyRegistry.add("camel.component.kafka.max-poll-records", () -> "1");
        dynamicPropertyRegistry.add("camel.main.auto-startup", () -> "true");
        dynamicPropertyRegistry.add("camel.component.kafka.request-required-acks", () -> "all");
        dynamicPropertyRegistry.add("camel.component.kafka.key-serializer", () -> "org.apache.kafka.common.serialization.StringSerializer");
        dynamicPropertyRegistry.add("camel.component.kafka.value-serializer", () -> "io.apicurio.registry.serde.avro.AvroKafkaSerializer");
        dynamicPropertyRegistry.add("camel.component.kafka.key-deserializer", () -> "org.apache.kafka.common.serialization.StringDeserializer");
        dynamicPropertyRegistry.add("camel.component.kafka.value-deserializer", () -> "io.apicurio.registry.serde.avro.AvroKafkaDeserializer");
        dynamicPropertyRegistry.add("camel.component.kafka.additional-properties[apicurio.registry.url]", () -> "http://localhost:" + registryPort + "/apis/registry/v3");
        dynamicPropertyRegistry.add("camel.component.kafka.additional-properties[apicurio.registry.auto-register]", () -> "true");
        dynamicPropertyRegistry.add("camel.component.kafka.additional-properties[apicurio.registry.use-specific-avro-reader]", () -> "true");
        dynamicPropertyRegistry.add("gregor.kafka.listener-concurrency", () -> "1");
        dynamicPropertyRegistry.add("gregor.webhook.tenant-id", () -> TENANT_ID);
        dynamicPropertyRegistry.add("gregor.webhook.endpoint-id", () -> ENDPOINT_ID);
        dynamicPropertyRegistry.add("gregor.webhook.receiver-url", TEST_RECEIVER::uri);
        dynamicPropertyRegistry.add("gregor.webhook.signing-secret", () -> SIGNING_SECRET);
        dynamicPropertyRegistry.add("gregor.webhook.connection-timeout", () -> "2s");
        dynamicPropertyRegistry.add("gregor.webhook.response-timeout", () -> "5s");
        dynamicPropertyRegistry.add("gregor.webhook.receiver-timestamp-tolerance", () -> "5m");
    }

    @AfterAll
    static void stopReceiver() {
        TEST_RECEIVER.close();
    }

    @Test
    void discardsInvalidSourceEnvelopesWithoutDeliveryAndCommitsTheirOffsets() throws Exception {
        List<WebhookEvent> impossibleEvents = List.of(
                event("exhausted-attempt-count.test", MAXIMUM_ATTEMPTS, 0, 0, null),
                event("negative-attempt-count.test", -1, 0, 0, null),
                event("negative-retry-count.test", 0, -1, 0, null),
                event("negative-replay-count.test", 0, 0, -1, null)
        );
        TopicPartition topicPartition = new TopicPartition(WebhookRoute.WEBHOOK_EVENT_TOPIC, 0);
        try (AdminClient adminClient = KafkaTestRecords.adminClient()) {
            Map<TopicPartition, Long> downstreamOffsetsBefore = KafkaTestRecords.endOffsets(adminClient, DOWNSTREAM_TOPICS);
            for (WebhookEvent impossibleWebhookEvent : impossibleEvents) {
                this.assertDiscarded(adminClient, topicPartition, impossibleWebhookEvent);
            }
            Assertions.assertThat(KafkaTestRecords.endOffsets(adminClient, DOWNSTREAM_TOPICS)).isEqualTo(downstreamOffsetsBefore);
        }
    }

    @Test
    void discardsMalformedRetryEnvelopesWithoutDeliveryOrDownstreamPublicationAndCommitsOffsets() throws Exception {
        Instant now = Instant.now();
        List<RetryRecord> malformedRetryRecords = List.of(
                new RetryRecord(WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC, event("retry-exhausted.test", MAXIMUM_ATTEMPTS, 0, 0, now)),
                new RetryRecord(WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC, event("retry-negative-attempt-count.test", -1, 0, 0, now)),
                new RetryRecord(WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC, event("retry-negative-retry-count.test", 0, -1, 0, now)),
                new RetryRecord(WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC, event("retry-negative-replay-count.test", 0, 0, -1, now)),
                new RetryRecord(WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC, event("retry-null-eligibility.test", 0, 0, 0, null)),
                new RetryRecord(WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC, event("retry-distant-eligibility.test", 0, 0, 0, now.plus(Duration.ofHours(2))))
        );
        try (AdminClient adminClient = KafkaTestRecords.adminClient()) {
            Map<TopicPartition, Long> expectedDownstreamOffsets = new HashMap<>(KafkaTestRecords.endOffsets(adminClient, DOWNSTREAM_TOPICS));
            for (RetryRecord malformedRetryRecord : malformedRetryRecords) {
                TopicPartition topicPartition = new TopicPartition(malformedRetryRecord.topic(), 0);
                this.assertDiscarded(adminClient, topicPartition, malformedRetryRecord.webhookEvent());
                expectedDownstreamOffsets.compute(topicPartition, (_, offset) -> offset + 1);
            }
            Assertions.assertThat(KafkaTestRecords.endOffsets(adminClient, DOWNSTREAM_TOPICS)).isEqualTo(expectedDownstreamOffsets);
        }
    }

    @Test
    void deliversAnEnvelopeWithAFutureNextEligibleAtWithoutDelayingOrSkippingIt() throws Exception {
        WebhookEvent eligibleLaterWebhookEvent = event("next-eligible-at.test", 0, 0, 0, Instant.now().plus(Duration.ofHours(2)));
        Instant sendStartedAt = Instant.now();

        this.producerTemplate.sendBodyAndHeaders("kafka:" + WebhookRoute.WEBHOOK_EVENT_TOPIC, eligibleLaterWebhookEvent, Map.of(KafkaConstants.KEY, ENDPOINT_ID, KafkaConstants.PARTITION_KEY, 0));

        TEST_RECEIVER.awaitRequest(eligibleLaterWebhookEvent.getEventId());
        Assertions.assertThat(Duration.between(sendStartedAt, Instant.now())).isLessThan(Duration.ofSeconds(5));
        Map<String, Object> attemptColumns = this.awaitAttemptRow(eligibleLaterWebhookEvent.getEventId());
        Assertions.assertThat(attemptColumns)
                .containsEntry("attempt_number", 1)
                .containsEntry("outcome", "SUCCEEDED")
                .containsEntry("http_status", 204)
                .containsEntry("retry_count", 0)
                .containsEntry("replay_count", 0)
                .containsEntry("dead_lettered", false);
    }

    @Test
    void deliversAnEnvelopeWhoseRetryCountExceedsItsDeliveryAttemptCountWithTheEnvelopeCounters() throws Exception {
        WebhookEvent aheadOfAttemptsWebhookEvent = event("retry-count-ahead.test", 0, 3, 2, null);

        this.producerTemplate.sendBodyAndHeaders("kafka:" + WebhookRoute.WEBHOOK_EVENT_TOPIC, aheadOfAttemptsWebhookEvent, Map.of(KafkaConstants.KEY, ENDPOINT_ID, KafkaConstants.PARTITION_KEY, 0));

        TEST_RECEIVER.awaitRequest(aheadOfAttemptsWebhookEvent.getEventId());
        Map<String, Object> attemptColumns = this.awaitAttemptRow(aheadOfAttemptsWebhookEvent.getEventId());
        Assertions.assertThat(attemptColumns).containsEntry("attempt_number", 1).containsEntry("outcome", "SUCCEEDED").containsEntry("retry_count", 3).containsEntry("replay_count", 2).containsEntry("dead_lettered", false);
    }

    private void assertDiscarded(AdminClient adminClient, TopicPartition topicPartition, WebhookEvent webhookEvent) throws Exception {
        long inputOffset = adminClient.listOffsets(Map.of(topicPartition, OffsetSpec.latest())).partitionResult(topicPartition).get(5, TimeUnit.SECONDS).offset();
        this.producerTemplate.sendBodyAndHeaders("kafka:" + topicPartition.topic(), webhookEvent, Map.of(KafkaConstants.KEY, ENDPOINT_ID, KafkaConstants.PARTITION_KEY, topicPartition.partition()));
        KafkaTestRecords.awaitCommit(adminClient, topicPartition, inputOffset);
        Assertions.assertThat(TEST_RECEIVER.count(webhookEvent.getEventId())).isZero();
        Assertions.assertThat(this.attemptRowCount(webhookEvent.getEventId())).isZero();
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
        Integer count = this.jdbcTemplate.queryForObject("SELECT count(*) FROM delivery_attempt WHERE event_id = ?::uuid", Integer.class, eventId);
        return count == null ? 0 : count;
    }

    private static WebhookEvent event(String eventType, int deliveryAttemptCount, int retryCount, int replayCount, Instant nextEligibleAt) {
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

    private record RetryRecord(String topic, WebhookEvent webhookEvent) {
    }

    private record CapturedRequest(String webhookId) {
    }

    private static final class TestReceiver implements AutoCloseable {

        private final HttpServer httpServer;
        private final ExecutorService executorService;
        private final ConcurrentLinkedQueue<CapturedRequest> capturedRequests = new ConcurrentLinkedQueue<>();

        private TestReceiver(HttpServer httpServer, ExecutorService executorService) {
            this.httpServer = httpServer;
            this.executorService = executorService;
        }

        static TestReceiver start() {
            try {
                HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
                ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
                TestReceiver testReceiver = new TestReceiver(httpServer, executorService);
                httpServer.createContext("/webhook", testReceiver::handle);
                httpServer.setExecutor(executorService);
                httpServer.start();
                return testReceiver;
            } catch (IOException exception) {
                throw new IllegalStateException("Could not start impossible-state receiver", exception);
            }
        }

        String uri() {
            return "http://localhost:" + this.httpServer.getAddress().getPort() + "/webhook";
        }

        int count(String eventId) {
            return (int) this.capturedRequests.stream().filter(capturedRequest -> eventId.equals(capturedRequest.webhookId())).count();
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

        private void handle(HttpExchange httpExchange) throws IOException {
            httpExchange.getRequestBody().readAllBytes();
            this.capturedRequests.add(new CapturedRequest(httpExchange.getRequestHeaders().getFirst("webhook-id")));
            try (httpExchange) {
                httpExchange.sendResponseHeaders(204, -1L);
            }
        }

        @Override
        public void close() {
            this.httpServer.stop(0);
            this.executorService.shutdownNow();
        }
    }
}
