package gregor.integration;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import gregor.avro.DeliveryAttempt;
import gregor.avro.DeliveryOutcome;
import gregor.avro.WebhookEvent;
import gregor.component.SignatureComponent;
import gregor.route.WebhookRoute;
import gregor.service.WebhookService;
import io.vertx.core.Vertx;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.spi.RouteController;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "gregor.webhook.maximum-attempts=4"
)
class CoreWebhookDeliveryLoopIntegrationTests {

    private static final String TENANT_ID = "00000000-0000-0000-0000-000000000001";
    private static final String ENDPOINT_ID = "00000000-0000-0000-0000-000000000002";
    private static final String SIGNING_SECRET = "whsec_" + Base64.getEncoder().encodeToString(
            "01234567890123456789012345678901".getBytes(StandardCharsets.UTF_8)
    );
    private static final TestReceiver RECEIVER = TestReceiver.start();
    private static final String RETRY_TOPIC = WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC;

    @LocalServerPort
    private int serverPort;
    @Autowired
    private CamelContext camelContext;
    @Autowired
    private ProducerTemplate producerTemplate;
    @Autowired
    private Vertx registryVertx;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private SignatureComponent signatureComponent;
    @MockitoSpyBean
    private WebhookService webhookService;
    @MockitoSpyBean
    private Clock clock;

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
        // Interrupt the pending wait before the first tier's real delay can elapse during shutdown.
        registry.add("camel.component.kafka.shutdown-timeout", () -> "1000");
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
        registry.add("gregor.kafka.listener-concurrency", () -> "12");
        registry.add("gregor.webhook.tenant-id", () -> TENANT_ID);
        registry.add("gregor.webhook.endpoint-id", () -> ENDPOINT_ID);
        registry.add("gregor.webhook.receiver-url", RECEIVER::uri);
        registry.add("gregor.webhook.signing-secret", () -> SIGNING_SECRET);
        registry.add("gregor.webhook.connection-timeout", () -> "2s");
        registry.add("gregor.webhook.response-timeout", () -> "5s");
        registry.add("gregor.webhook.receiver-timestamp-tolerance", () -> "5m");
    }

    @BeforeEach
    void resetReceiver() {
        RECEIVER.reset();
    }

    @AfterAll
    static void stopReceiver() {
        RECEIVER.close();
    }

    @Test
    void acceptsAndDeliversAnEndpointKeyedSpecificAvroEventThenPersistsItsAttempt() throws Exception {
        try (
                AdminClient adminClient = KafkaTestRecords.adminClient();
                KafkaConsumer<String, Object> eventConsumer =
                        this.observeNewRecords(adminClient, WebhookRoute.WEBHOOK_EVENT_TOPIC);
                KafkaConsumer<String, Object> attemptConsumer =
                        this.observeNewRecords(adminClient, WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC)
        ) {
            HttpResponse<String> httpResponse = this.postEvent(
                    "{\"eventType\":\"order.created\",\"payload\":{\"orderId\":\"123\",\"name\":\"é\"}}"
            );

            Assertions.assertThat(httpResponse.statusCode()).isEqualTo(202);
            String eventId = this.objectMapper.readTree(httpResponse.body()).get("eventId").asString();
            Assertions.assertThatCode(() -> UUID.fromString(eventId)).doesNotThrowAnyException();

            ConsumerRecord<String, Object> eventRecord = KafkaTestRecords.awaitRecord(eventConsumer, eventId);
            Assertions.assertThat(eventRecord.key()).isEqualTo(ENDPOINT_ID);
            Assertions.assertThat(eventRecord.value()).isInstanceOf(WebhookEvent.class);
            WebhookEvent webhookEvent = (WebhookEvent) eventRecord.value();
            Assertions.assertThat(webhookEvent.getEventId()).isEqualTo(eventId);
            Assertions.assertThat(webhookEvent.getEventType()).isEqualTo("order.created");
            Assertions.assertThat(webhookEvent.getTenantId()).isEqualTo(TENANT_ID);
            Assertions.assertThat(webhookEvent.getEndpointId()).isEqualTo(ENDPOINT_ID);
            Assertions.assertThat(webhookEvent.getCreatedAt()).isBeforeOrEqualTo(Instant.now());
            Assertions.assertThat(webhookEvent.getRetryCount()).isZero();
            Assertions.assertThat(webhookEvent.getNextEligibleAt()).isNull();
            Assertions.assertThat(webhookEvent.getDeliveryAttemptCount()).isZero();
            Assertions.assertThat(webhookEvent.getReplayCount()).isZero();
            Assertions.assertThat(bytes(webhookEvent.getPayload()))
                    .isEqualTo("{\"orderId\":\"123\",\"name\":\"é\"}".getBytes(StandardCharsets.UTF_8));

            CapturedRequest delivered = RECEIVER.awaitRequest(eventId, 1);
            Assertions.assertThat(delivered.body())
                    .isEqualTo("{\"orderId\":\"123\",\"name\":\"é\"}".getBytes(StandardCharsets.UTF_8));
            Assertions.assertThat(delivered.eventType()).isEqualTo("order.created");
            Assertions.assertThat(delivered.contentType()).startsWith("application/json");
            Assertions.assertThat(delivered.signature()).isEqualTo(this.signatureComponent.sign(
                    delivered.webhookId(),
                    Long.parseLong(delivered.timestamp()),
                    delivered.body()
            ));

            ConsumerRecord<String, Object> attemptRecord = KafkaTestRecords.awaitRecord(attemptConsumer, eventId);
            Assertions.assertThat(attemptRecord.key()).isEqualTo(eventId);
            Assertions.assertThat(attemptRecord.value()).isInstanceOf(DeliveryAttempt.class);
            DeliveryAttempt attempt = (DeliveryAttempt) attemptRecord.value();
            Assertions.assertThatCode(() -> UUID.fromString(attempt.getAttemptId())).doesNotThrowAnyException();
            Assertions.assertThat(attempt.getEventId()).isEqualTo(eventId);
            Assertions.assertThat(attempt.getAttemptNumber()).isEqualTo(1);
            Assertions.assertThat(attempt.getOutcome()).isEqualTo(DeliveryOutcome.SUCCEEDED);
            Assertions.assertThat(attempt.getHttpStatus()).isEqualTo(204);
            Assertions.assertThat(attempt.getLatencyMs()).isNotNegative();
            Assertions.assertThat(attempt.getAttemptedAt()).isBeforeOrEqualTo(Instant.now());
            Assertions.assertThat(attempt.getRetryCount()).isZero();
            Assertions.assertThat(attempt.getReplayCount()).isZero();
            Assertions.assertThat(attempt.getDeadLettered()).isFalse();

            this.awaitAttemptRows(eventId, 1);
            Map<String, Object> row = this.jdbcTemplate.queryForMap(
                    """
                    SELECT tenant_id, endpoint_id, event_type, attempt_number, outcome, http_status,
                           latency_ms, attempted_at, retry_count, replay_count, dead_lettered,
                           details::text AS details
                    FROM delivery_attempt
                    WHERE event_id = ?::uuid
                    """,
                    eventId
            );
            Assertions.assertThat(row.get("tenant_id").toString()).isEqualTo(TENANT_ID);
            Assertions.assertThat(row.get("endpoint_id").toString()).isEqualTo(ENDPOINT_ID);
            Assertions.assertThat(row)
                    .containsEntry("event_type", "order.created")
                    .containsEntry("attempt_number", 1)
                    .containsEntry("outcome", "SUCCEEDED")
                    .containsEntry("http_status", 204);
            Assertions.assertThat((Long) row.get("latency_ms")).isNotNegative();
            assertPhaseOneAuditFields(row);
            Assertions.assertThat(row.get("details").toString()).isEqualTo("{}");
        }
    }

    @Test
    void acceptsEveryNonNullPayloadTypeWithoutDeduplicatingRepeatedRequests() throws Exception {
        List<String> payloads = List.of("{}", "[1]", "\"value\"", "42", "true");
        List<String> eventIds = new ArrayList<>();

        for (String payload : payloads) {
            HttpResponse<String> response =
                    this.postEvent("{\"eventType\":\"payload.test\",\"payload\":" + payload + "}");
            Assertions.assertThat(response.statusCode()).isEqualTo(202);
            eventIds.add(this.objectMapper.readTree(response.body()).get("eventId").asString());
        }

        HttpResponse<String> repeated = this.postEvent("{\"eventType\":\"payload.test\",\"payload\":true}");
        Assertions.assertThat(repeated.statusCode()).isEqualTo(202);
        eventIds.add(this.objectMapper.readTree(repeated.body()).get("eventId").asString());
        Assertions.assertThat(eventIds).doesNotHaveDuplicates();
        for (String eventId : eventIds) {
            RECEIVER.awaitRequest(eventId, 1);
            this.awaitAttemptRows(eventId, 1);
        }
    }

    @Test
    void rejectsMalformedAndInvalidIngestRequestsWithoutPublishingAnEvent() throws Exception {
        List<String> invalidBodies = List.of(
                "{\"eventType\":",
                "{}",
                "{\"eventType\":\" \",\"payload\":{}}",
                "{\"eventType\":\"order.\\r\\ncreated\",\"payload\":{}}",
                "{\"eventType\":\"order.\\u0000created\",\"payload\":{}}",
                "{\"eventType\":\"order.created\"}",
                "{\"eventType\":\"order.created\",\"payload\":null}"
        );
        String topic = WebhookRoute.WEBHOOK_EVENT_TOPIC;
        try (AdminClient adminClient = KafkaTestRecords.adminClient()) {
            Map<TopicPartition, Long> offsetsBefore = KafkaTestRecords.endOffsets(adminClient, topic);
            for (String body : invalidBodies) {
                HttpResponse<String> response = this.postEvent(body);
                Assertions.assertThat(response.statusCode()).as("response for %s", body).isEqualTo(400);
                Assertions.assertThat(response.body())
                        .contains("\"title\":\"Invalid request\"")
                        .contains("\"detail\":\"Request body is malformed or invalid.\"");
            }
            Assertions.assertThat(KafkaTestRecords.endOffsets(adminClient, topic)).isEqualTo(offsetsBefore);
        }
    }

    @Test
    void deliversALaterSourceEventBeforeThePendingRetryAndResumesTheRetryAfterRestart() throws Exception {
        Instant initial = Instant.parse("2026-07-25T12:00:00.123Z");
        Instant due = Instant.parse("2026-07-25T12:01:00.123Z");
        AtomicReference<Instant> now = new AtomicReference<>(initial);
        Mockito.doAnswer(_ -> now.get()).when(this.clock).instant();
        WebhookEvent first = event("delayed-first.test", ENDPOINT_ID);
        WebhookEvent later = event("delayed-later.test", ENDPOINT_ID);
        String eventId = first.getEventId();
        CountDownLatch eligibilityStarted = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            eligibilityStarted.countDown();
            return invocation.callRealMethod();
        }).when(this.webhookService).awaitEligibility(Mockito.argThat(
                message -> eventId.equals(message.webhookEvent().getEventId())
        ));
        String retryRoute = KafkaTestRecords.consumerRoute(RETRY_TOPIC);
        RouteController routeController = this.camelContext.getRouteController();
        String sourceTopic = WebhookRoute.WEBHOOK_EVENT_TOPIC;
        try (AdminClient adminClient = KafkaTestRecords.adminClient()) {
            Map<TopicPartition, Long> sourceOffsets = KafkaTestRecords.endOffsets(adminClient, sourceTopic);
            Map<TopicPartition, Long> retryOffsets = KafkaTestRecords.retryEndOffsets(adminClient);
            try (
                    AutoCloseable _ = () -> {
                        RECEIVER.clearRetryableFailure();
                        try {
                            KafkaTestRecords.awaitPublishedCommits(adminClient, sourceTopic, sourceOffsets);
                        } finally {
                            KafkaTestRecords.drainRetries(adminClient, routeController, retryOffsets, now);
                            if (routeController.getRouteStatus(retryRoute).isStopped()) {
                                routeController.startRoute(retryRoute);
                            }
                        }
                    };
                    KafkaConsumer<String, Object> source =
                            KafkaTestRecords.consumer(this.registryVertx, sourceOffsets);
                    KafkaConsumer<String, Object> delayed =
                            KafkaTestRecords.consumer(this.registryVertx, retryOffsets)
            ) {
                RECEIVER.failOnly(eventId);
                this.sendToPartition(first, 0);
                KafkaTestRecords.awaitCommit(adminClient, KafkaTestRecords.awaitRecord(source, eventId));
                ConsumerRecord<String, Object> pending = KafkaTestRecords.awaitRecord(delayed, eventId);
                Assertions.assertThat(pending.topic()).isEqualTo(RETRY_TOPIC);
                WebhookEvent successor = (WebhookEvent) pending.value();
                Assertions.assertThat(pending.key()).isEqualTo(first.getEndpointId());
                Assertions.assertThat(successor.getNextEligibleAt()).isEqualTo(due);
                Assertions.assertThat(eligibilityStarted.await(15, TimeUnit.SECONDS)).isTrue();

                this.sendToPartition(later, 0);
                KafkaTestRecords.awaitCommit(adminClient, KafkaTestRecords.awaitRecord(source, later.getEventId()));
                RECEIVER.awaitRequest(later.getEventId(), 1);
                this.awaitAttemptRows(later.getEventId(), 1);
                Assertions.assertThat(this.jdbcTemplate.queryForObject(
                        "SELECT outcome FROM delivery_attempt WHERE event_id = ?::uuid",
                        String.class,
                        later.getEventId()
                )).isEqualTo("SUCCEEDED");
                Assertions.assertThat(now.get()).isBefore(due);
                Assertions.assertThat(RECEIVER.count(eventId)).isEqualTo(1);
                this.awaitAttemptRows(eventId, 1);

                KafkaTestRecords.stopRetryConsumer(routeController, retryRoute);
                Assertions.assertThat(RECEIVER.count(eventId)).isEqualTo(1);
                RECEIVER.clearRetryableFailure();
                now.set(due);
                routeController.startRoute(retryRoute);
                CapturedRequest retried = RECEIVER.awaitRequest(eventId, 2);
                KafkaTestRecords.awaitCommit(adminClient, pending);
                this.awaitAttemptRows(eventId, 2);
                Assertions.assertThat(RECEIVER.count(eventId)).isEqualTo(2);
                Assertions.assertThat(retried.webhookId()).isEqualTo(eventId);
                Assertions.assertThat(retried.timestamp()).isEqualTo(Long.toString(due.getEpochSecond()));
                Assertions.assertThat(retried.body()).isEqualTo(bytes(first.getPayload()));
                Assertions.assertThat(this.jdbcTemplate.queryForList(
                        """
                        SELECT attempt_number, retry_count, replay_count, outcome
                        FROM delivery_attempt WHERE event_id = ?::uuid ORDER BY attempt_number
                        """,
                        eventId
                )).containsExactly(
                        Map.of("attempt_number", 1, "retry_count", 0, "replay_count", 0, "outcome", "RETRYABLE_FAILURE"),
                        Map.of("attempt_number", 2, "retry_count", 1, "replay_count", 0, "outcome", "SUCCEEDED")
                );
            }
        }
    }

    @Test
    void treatsRedirectsAsPermanentFailuresWithoutFollowingThem() throws Exception {
        RECEIVER.respondWith(302);
        try (
                AdminClient adminClient = KafkaTestRecords.adminClient();
                KafkaConsumer<String, Object> source = this.observeNewRecords(
                        adminClient,
                        WebhookRoute.WEBHOOK_EVENT_TOPIC
                )
        ) {

            HttpResponse<String> httpResponse = this.postEvent("{\"eventType\":\"redirect.test\",\"payload\":{}}");
            Assertions.assertThat(httpResponse.statusCode()).isEqualTo(202);
            String eventId = this.objectMapper.readTree(httpResponse.body()).get("eventId").asString();

            KafkaTestRecords.awaitCommit(adminClient, KafkaTestRecords.awaitRecord(source, eventId));
            this.awaitAttemptRows(eventId, 1);
            Assertions.assertThat(RECEIVER.count(eventId)).isEqualTo(1);
            Assertions.assertThat(this.jdbcTemplate.queryForMap(
                    "SELECT outcome, http_status FROM delivery_attempt WHERE event_id = ?::uuid", eventId
            )).containsEntry("outcome", "PERMANENT_FAILURE").containsEntry("http_status", 302);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void auditsOneTransportFailureWithNoHttpStatusWhenNoHigherTierExists(boolean timeout) throws Exception {
        WebhookEvent event = WebhookEvent.newBuilder(event("transport.test", ENDPOINT_ID))
                .setRetryCount(3)
                .setDeliveryAttemptCount(3)
                .setNextEligibleAt(Instant.EPOCH)
                .build();
        WebhookEvent unrelated = WebhookEvent.newBuilder(event).setEventId(UUID.randomUUID().toString()).build();
        String topic = WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC;
        RECEIVER.failTransportOnly(event.getEventId(), timeout);
        try (
                AdminClient adminClient = KafkaTestRecords.adminClient();
                KafkaConsumer<String, Object> input = this.observeNewRecords(adminClient, topic)
        ) {
            for (WebhookEvent message : List.of(unrelated, event)) {
                this.producerTemplate.sendBodyAndHeader(
                        "kafka:" + topic, message, KafkaConstants.KEY, message.getEndpointId()
                );
                KafkaTestRecords.awaitCommit(adminClient, KafkaTestRecords.awaitRecord(input, message.getEventId()));
            }
        } finally {
            RECEIVER.clearTransportFailure();
        }
        this.awaitAttemptRows(unrelated.getEventId(), 1);
        Assertions.assertThat(this.jdbcTemplate.queryForObject(
                "SELECT outcome FROM delivery_attempt WHERE event_id = ?::uuid", String.class, unrelated.getEventId()
        )).isEqualTo("SUCCEEDED");
        this.awaitAttemptRows(event.getEventId(), 1);
        Assertions.assertThat(RECEIVER.count(event.getEventId())).isEqualTo(1);
        Map<String, Object> row = this.jdbcTemplate.queryForMap(
                """
                SELECT attempt_number, outcome, http_status, retry_count, replay_count, dead_lettered,
                       details::text AS details
                FROM delivery_attempt WHERE event_id = ?::uuid
                """,
                event.getEventId()
        );
        Assertions.assertThat(row)
                .containsEntry("attempt_number", 4)
                .containsEntry("outcome", "RETRYABLE_FAILURE")
                .containsEntry("http_status", null)
                .containsEntry("retry_count", 3)
                .containsEntry("replay_count", 0)
                .containsEntry("dead_lettered", false);
        JsonNode details = this.objectMapper.readTree(row.get("details").toString());
        Assertions.assertThat(details.get("type").asString()).isIn(
                timeout ? List.of("TIMEOUT") : List.of("CONNECTION_FAILURE", "INVALID_RESPONSE")
        );
        Assertions.assertThat(details.has("exceptionClass")).isTrue();
        Assertions.assertThat(details.has("message")).isTrue();
    }

    @Test
    void omitsHttpFailureDetailsThatContainTheRequestPayloadOrSigningSecret() throws Exception {
        String payload = "{\"private\":\"request-payload-marker\"}";
        RECEIVER.respondWith(400, "echo=" + payload + ";secret=" + SIGNING_SECRET);

        HttpResponse<String> response = this.postEvent(
                "{\"eventType\":\"sensitive-details.test\",\"payload\":" + payload + "}"
        );
        String eventId = this.objectMapper.readTree(response.body()).get("eventId").asString();
        RECEIVER.awaitRequest(eventId, 1);
        this.awaitAttemptRows(eventId, 1);

        Map<String, Object> row = this.jdbcTemplate.queryForMap(
                """
                SELECT retry_count, replay_count, dead_lettered, details::text AS details
                FROM delivery_attempt
                WHERE event_id = ?::uuid
                """,
                eventId
        );
        assertPhaseOneAuditFields(row);
        String detailsJson = row.get("details").toString();
        Assertions.assertThat(this.objectMapper.readTree(detailsJson))
                .isEqualTo(this.objectMapper.readTree("{\"type\":\"HTTP_FAILURE\"}"));
        Assertions.assertThat(detailsJson).doesNotContain(payload, SIGNING_SECRET);
    }

    @Test
    void processesOnePartitionSequentiallyAndDifferentPartitionsConcurrently() throws Exception {
        this.awaitPartitionsAssignedToDifferentConsumers(
                WebhookRoute.WEBHOOK_EVENT_TOPIC,
                WebhookRoute.WEBHOOK_EVENT_TOPIC,
                0,
                1
        );
        WebhookEvent first = event("same-partition-first", "endpoint-a");
        WebhookEvent second = event("same-partition-second", "endpoint-a");
        WebhookEvent parallel = event("different-partition", "endpoint-b");
        RECEIVER.blockResponses();
        try {
            this.sendToPartition(first, 0);
            this.sendToPartition(second, 0);
            this.sendToPartition(parallel, 1);

            RECEIVER.awaitRequests(first.getEventId(), parallel.getEventId());
            Assertions.assertThat(RECEIVER.count(second.getEventId())).isZero();
        } finally {
            RECEIVER.releaseResponses();
        }

        for (WebhookEvent event : List.of(first, second, parallel)) {
            this.awaitAttemptRows(event.getEventId(), 1);
        }
    }

    private void sendToPartition(WebhookEvent event, int partition) {
        this.producerTemplate.sendBodyAndHeaders(
                "kafka:" + WebhookRoute.WEBHOOK_EVENT_TOPIC,
                event,
                Map.of(KafkaConstants.KEY, event.getEndpointId(), KafkaConstants.PARTITION_KEY, partition)
        );
    }

    @Test
    void duplicateSourceDeliveryPostsAgainWithoutDuplicatingThePersistedAttemptNumber() throws Exception {
        String eventId = UUID.randomUUID().toString();
        WebhookEvent event = new WebhookEvent(
                eventId,
                "duplicate.test",
                TENANT_ID,
                ENDPOINT_ID,
                Instant.now(),
                ByteBuffer.wrap("{\"duplicate\":true}".getBytes(StandardCharsets.UTF_8)),
                0,
                null,
                0,
                0
        );

        try (
                AdminClient admin = KafkaTestRecords.adminClient();
                KafkaConsumer<String, Object> attempts =
                        this.observeNewRecords(admin, WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC)
        ) {
            Map<String, Object> headers = Map.of(KafkaConstants.KEY, event.getEndpointId());
            this.producerTemplate.sendBodyAndHeaders("kafka:" + WebhookRoute.WEBHOOK_EVENT_TOPIC, event, headers);
            KafkaTestRecords.awaitCommit(admin, KafkaTestRecords.awaitRecord(attempts, eventId));

            this.producerTemplate.sendBodyAndHeaders("kafka:" + WebhookRoute.WEBHOOK_EVENT_TOPIC, event, headers);
            KafkaTestRecords.awaitCommit(admin, KafkaTestRecords.awaitRecord(attempts, eventId));

            Assertions.assertThat(RECEIVER.count(eventId)).isEqualTo(2);
            Assertions.assertThat(this.attemptRowCount(eventId)).isEqualTo(1);
        }
    }

    @Test
    void persistsPostgresqlJsonbAndTreatsDuplicateAttemptEnvelopesAsSuccess() {
        String attemptId = UUID.randomUUID().toString();
        String eventId = UUID.randomUUID().toString();
        DeliveryAttempt attempt = new DeliveryAttempt(
                attemptId,
                eventId,
                TENANT_ID,
                ENDPOINT_ID,
                "persistence.test",
                3,
                DeliveryOutcome.RETRYABLE_FAILURE,
                null,
                42L,
                Instant.parse("2026-07-10T12:00:00Z"),
                ByteBuffer.wrap("{\"type\":\"TIMEOUT\"}".getBytes(StandardCharsets.UTF_8)),
                2,
                1,
                true
        );
        DeliveryAttempt duplicateAttemptNumber = new DeliveryAttempt(
                UUID.randomUUID().toString(),
                eventId,
                TENANT_ID,
                ENDPOINT_ID,
                "duplicate.test",
                3,
                DeliveryOutcome.SUCCEEDED,
                204,
                1L,
                Instant.parse("2026-07-10T12:01:00Z"),
                ByteBuffer.wrap("{}".getBytes(StandardCharsets.UTF_8)),
                0,
                0,
                false
        );

        Assertions.assertThatCode(() -> {
            this.webhookService.persistDeliveryAttempt(attempt);
            this.webhookService.persistDeliveryAttempt(duplicateAttemptNumber);
        }).doesNotThrowAnyException();

        Assertions.assertThat(this.attemptRowCount(eventId)).isEqualTo(1);
        String dataType = this.jdbcTemplate.queryForObject(
                """
                SELECT data_type
                FROM information_schema.columns
                WHERE table_name = 'delivery_attempt' AND column_name = 'details'
                """,
                String.class
        );
        Assertions.assertThat(dataType).isEqualTo("jsonb");
        Map<String, Object> row = this.jdbcTemplate.queryForMap(
                """
                SELECT attempt_id, event_id, tenant_id, endpoint_id, event_type, attempt_number, outcome,
                       http_status, latency_ms, extract(epoch from attempted_at)::bigint AS attempted_at_epoch,
                       retry_count, replay_count, dead_lettered, details::text AS details
                FROM delivery_attempt
                WHERE event_id = ?::uuid
                """,
                eventId
        );
        Assertions.assertThat(row.get("attempt_id").toString()).isEqualTo(attemptId);
        Assertions.assertThat(row.get("event_id").toString()).isEqualTo(eventId);
        Assertions.assertThat(row.get("tenant_id").toString()).isEqualTo(TENANT_ID);
        Assertions.assertThat(row.get("endpoint_id").toString()).isEqualTo(ENDPOINT_ID);
        Assertions.assertThat(row)
                .containsEntry("event_type", "persistence.test")
                .containsEntry("attempted_at_epoch", Instant.parse("2026-07-10T12:00:00Z").getEpochSecond())
                .containsEntry("attempt_number", 3)
                .containsEntry("outcome", "RETRYABLE_FAILURE")
                .containsEntry("latency_ms", 42L);
        Assertions.assertThat(row.get("http_status")).isNull();
        Assertions.assertThat(row)
                .containsEntry("retry_count", 2)
                .containsEntry("replay_count", 1)
                .containsEntry("dead_lettered", true);
        Assertions.assertThat(row.get("details").toString()).contains("\"type\": \"TIMEOUT\"");

        List<Map<String, Object>> auditColumns = this.jdbcTemplate.queryForList(
                """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'delivery_attempt'
                  AND column_name IN ('retry_count', 'replay_count', 'dead_lettered')
                ORDER BY column_name
                """
        );
        Assertions.assertThat(auditColumns)
                .extracting(column -> List.of(
                        column.get("column_name"),
                        column.get("data_type"),
                        column.get("is_nullable")
                ))
                .containsExactly(
                        List.of("dead_lettered", "boolean", "NO"),
                        List.of("replay_count", "integer", "NO"),
                        List.of("retry_count", "integer", "NO")
                );
        List<String> columnNames = this.jdbcTemplate.queryForList(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'delivery_attempt'
                """,
                String.class
        );
        Assertions.assertThat(columnNames).doesNotContain("payload", "signing_secret", "completed_at");
    }

    @Test
    void declaresTopicsWithTheExpectedPartitioningAndRetention() throws Exception {
        List<String> expectedTopics = List.of(
                "webhook-events", "persist-delivery-attempt", "webhook-events-retry-1m", "webhook-events-retry-10m",
                "webhook-events-retry-1h", "webhook-events-dead-letter"
        );
        try (AdminClient adminClient = KafkaTestRecords.adminClient()) {
            Map<String, TopicDescription> descriptions =
                    adminClient.describeTopics(expectedTopics).allTopicNames().get(10, TimeUnit.SECONDS);
            Assertions.assertThat(descriptions.keySet()).containsExactlyInAnyOrderElementsOf(expectedTopics);
            Assertions.assertThat(descriptions.values()).allSatisfy(description -> {
                Assertions.assertThat(description.partitions()).hasSize(12);
                Assertions.assertThat(description.partitions()).allSatisfy(partition ->
                        Assertions.assertThat(partition.replicas()).hasSize(1)
                );
            });

            Map<String, String> expectedRetentions = Map.of(
                    "webhook-events", "604800000",
                    "persist-delivery-attempt", "2592000000",
                    "webhook-events-retry-1m", "604800000",
                    "webhook-events-retry-10m", "604800000",
                    "webhook-events-retry-1h", "604800000",
                    "webhook-events-dead-letter", "2592000000"
            );
            List<ConfigResource> configResources = expectedTopics.stream()
                    .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic))
                    .toList();
            Map<ConfigResource, Config> configs = adminClient
                    .describeConfigs(configResources)
                    .all()
                    .get(10, TimeUnit.SECONDS);
            for (ConfigResource configResource : configResources) {
                Assertions.assertThat(configValue(configs.get(configResource), TopicConfig.CLEANUP_POLICY_CONFIG))
                        .isEqualTo(TopicConfig.CLEANUP_POLICY_DELETE);
                Assertions.assertThat(configValue(configs.get(configResource), TopicConfig.RETENTION_MS_CONFIG))
                        .isEqualTo(expectedRetentions.get(configResource.name()));
            }

            Assertions.assertThat(this.camelContext.getRoutes()).noneMatch(route ->
                    route.getEndpoint().getEndpointUri().startsWith(
                            "kafka:" + WebhookRoute.WEBHOOK_EVENT_DEAD_LETTER_TOPIC
                    )
            );
        }
    }

    @Test
    void servesTheMockWebhookReceiverWithTheConfiguredSecret() throws Exception {
        byte[] body = "{\"receiver\":\"é\"}".getBytes(StandardCharsets.UTF_8);
        long now = Instant.now().getEpochSecond();
        String valid = this.signatureComponent.sign("receiver-event", now, body);

        Assertions.assertThat(this.postReceiver(body, "receiver-event", Long.toString(now), valid).statusCode())
                .isEqualTo(204);
    }

    private HttpResponse<String> postEvent(String body) throws IOException, InterruptedException {
        HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + this.serverPort + "/api/events"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
        return HttpClient.newHttpClient().send(httpRequest, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> postReceiver(
            byte[] body,
            String webhookId,
            String timestamp,
            String signature
    ) throws IOException, InterruptedException {
        HttpRequest httpRequest = HttpRequest
                .newBuilder()
                .uri(URI.create("http://localhost:" + this.serverPort + "/api/mocked-webhook-receiver"))
                .header("Content-Type", "application/json")
                .header("webhook-id", webhookId)
                .header("webhook-timestamp", timestamp)
                .header("webhook-signature", signature)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        return HttpClient.newHttpClient().send(httpRequest, HttpResponse.BodyHandlers.ofString());
    }

    private KafkaConsumer<String, Object> observeNewRecords(AdminClient admin, String topic) throws Exception {
        return KafkaTestRecords.consumer(this.registryVertx, KafkaTestRecords.endOffsets(admin, topic));
    }

    @SuppressWarnings("SameParameterValue")
    private void awaitPartitionsAssignedToDifferentConsumers(
            String groupId,
            String topic,
            int firstPartition,
            int secondPartition
    ) throws InterruptedException {
        TopicPartition first = new TopicPartition(topic, firstPartition);
        TopicPartition second = new TopicPartition(topic, secondPartition);
        Instant deadline = Instant.now().plusSeconds(15);
        try (AdminClient admin = KafkaTestRecords.adminClient()) {
            while (Instant.now().isBefore(deadline)) {
                try {
                    ConsumerGroupDescription description = admin
                            .describeConsumerGroups(List.of(groupId))
                            .all()
                            .get(2, TimeUnit.SECONDS)
                            .get(groupId);
                    MemberDescription firstOwner = ownerOf(description, first);
                    MemberDescription secondOwner = ownerOf(description, second);
                    if (description.groupState() == GroupState.STABLE
                            && description.members().size() == 12
                            && firstOwner != null
                            && secondOwner != null
                            && !firstOwner.consumerId().equals(secondOwner.consumerId())) {
                        return;
                    }
                } catch (ExecutionException | TimeoutException ignored) {
                    // The group can be absent or rebalancing while the application consumers start.
                }
                Thread.sleep(50L);
            }
        }
        throw new AssertionError("Partitions " + firstPartition + " and " + secondPartition
                + " were not assigned to different consumers in stable group " + groupId);
    }

    private static MemberDescription ownerOf(
            ConsumerGroupDescription description,
            TopicPartition topicPartition
    ) {
        return description.members().stream()
                .filter(member -> member.assignment().topicPartitions().contains(topicPartition))
                .findFirst()
                .orElse(null);
    }

    private void awaitAttemptRows(String eventId, int expectedCount) throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(15);
        while (Instant.now().isBefore(deadline)) {
            if (this.attemptRowCount(eventId) == expectedCount) {
                return;
            }
            Thread.sleep(50L);
        }
        Assertions.fail("Expected " + expectedCount + " attempt rows for event " + eventId);
    }

    private int attemptRowCount(String eventId) {
        Integer count = this.jdbcTemplate.queryForObject(
                "SELECT count(*) FROM delivery_attempt WHERE event_id = ?::uuid",
                Integer.class,
                eventId
        );
        return count == null ? 0 : count;
    }

    private static void assertPhaseOneAuditFields(Map<String, Object> row) {
        Assertions.assertThat(row)
                .containsEntry("retry_count", 0)
                .containsEntry("replay_count", 0)
                .containsEntry("dead_lettered", false);
    }

    private static byte[] bytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }

    private static WebhookEvent event(String eventType, String endpointId) {
        return new WebhookEvent(
                UUID.randomUUID().toString(),
                eventType,
                TENANT_ID,
                UUID.nameUUIDFromBytes(endpointId.getBytes(StandardCharsets.UTF_8)).toString(),
                Instant.now(),
                ByteBuffer.wrap(("{\"eventType\":\"" + eventType + "\"}").getBytes(StandardCharsets.UTF_8)),
                0,
                null,
                0,
                0
        );
    }

    private static String configValue(Config config, String key) {
        ConfigEntry entry = config.get(key);
        return entry == null ? null : entry.value();
    }

    private record CapturedRequest(
            byte[] body,
            String eventType,
            String webhookId,
            String timestamp,
            String signature,
            String contentType
    ) {
    }

    private static final class TestReceiver implements AutoCloseable {

        private final HttpServer server;
        private final ExecutorService executor;
        private final ConcurrentLinkedQueue<CapturedRequest> requests = new ConcurrentLinkedQueue<>();
        private final AtomicInteger status = new AtomicInteger(204);
        private volatile String retryableEventId;
        private volatile TransportFailure transportFailure;
        private volatile byte[] responseBody = new byte[0];
        private volatile CountDownLatch responseGate;

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
                throw new IllegalStateException("Could not start integration receiver", exception);
            }
        }

        String uri() {
            return "http://localhost:" + this.server.getAddress().getPort() + "/webhook";
        }

        void respondWith(int responseStatus) {
            this.status.set(responseStatus);
        }

        void respondWith(int responseStatus, String body) {
            this.status.set(responseStatus);
            this.responseBody = body.getBytes(StandardCharsets.UTF_8);
        }

        void failOnly(String eventId) {
            this.retryableEventId = eventId;
        }

        void clearRetryableFailure() {
            this.retryableEventId = null;
        }

        void failTransportOnly(String eventId, boolean timeout) {
            this.transportFailure = new TransportFailure(eventId, timeout);
        }

        void clearTransportFailure() {
            this.transportFailure = null;
        }

        void blockResponses() {
            this.responseGate = new CountDownLatch(1);
        }

        void releaseResponses() {
            CountDownLatch gate = this.responseGate;
            if (gate != null) {
                gate.countDown();
            }
        }

        void reset() {
            this.releaseResponses();
            this.requests.clear();
            this.status.set(204);
            this.clearRetryableFailure();
            this.clearTransportFailure();
            this.responseBody = new byte[0];
            this.responseGate = null;
        }

        int count(String eventId) {
            return (int) this.requests.stream().filter(request -> eventId.equals(request.webhookId())).count();
        }

        CapturedRequest awaitRequest(String eventId, int expectedCount) throws InterruptedException {
            Instant deadline = Instant.now().plusSeconds(30);
            while (Instant.now().isBefore(deadline)) {
                List<CapturedRequest> matching = this.requests.stream()
                        .filter(request -> eventId.equals(request.webhookId()))
                        .toList();
                if (matching.size() >= expectedCount) {
                    return matching.get(expectedCount - 1);
                }
                Thread.sleep(25L);
            }
            throw new AssertionError("Expected " + expectedCount + " requests for event " + eventId);
        }

        void awaitRequests(String... eventIds) throws InterruptedException {
            Instant deadline = Instant.now().plusSeconds(4);
            while (Instant.now().isBefore(deadline)) {
                boolean allReceived = Stream.of(eventIds).allMatch(eventId -> this.count(eventId) >= 1);
                if (allReceived) {
                    return;
                }
                Thread.sleep(25L);
            }
            throw new AssertionError("Expected requests for events " + List.of(eventIds));
        }

        private void handle(HttpExchange exchange) throws IOException {
            byte[] body = exchange.getRequestBody().readAllBytes();
            String eventId = exchange.getRequestHeaders().getFirst("webhook-id");
            this.requests.add(new CapturedRequest(
                    body,
                    exchange.getRequestHeaders().getFirst("webhook-event-type"),
                    eventId,
                    exchange.getRequestHeaders().getFirst("webhook-timestamp"),
                    exchange.getRequestHeaders().getFirst("webhook-signature"),
                    exchange.getRequestHeaders().getFirst("Content-Type")
            ));
            try (exchange) {
                TransportFailure failure = this.transportFailure;
                if (failure != null && failure.eventId().equals(eventId)) {
                    if (!failure.timeout()) {
                        return;
                    }
                    Thread.sleep(Duration.ofSeconds(6));
                }
                CountDownLatch gate = this.responseGate;
                if (gate != null) {
                    gate.await(10, TimeUnit.SECONDS);
                }
                int responseStatus = eventId != null && eventId.equals(this.retryableEventId) ? 503 : this.status.get();
                if (responseStatus == 302) {
                    exchange.getResponseHeaders().add("Location", "/redirect-target");
                }
                byte[] responseBytes = this.responseBody;
                exchange.sendResponseHeaders(responseStatus, responseBytes.length == 0 ? -1L : responseBytes.length);
                if (responseBytes.length > 0) {
                    exchange.getResponseBody().write(responseBytes);
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void close() {
            this.releaseResponses();
            this.server.stop(0);
            this.executor.shutdownNow();
        }

        private record TransportFailure(String eventId, boolean timeout) {
        }
    }
}
