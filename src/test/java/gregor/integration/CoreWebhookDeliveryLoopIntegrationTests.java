package gregor.integration;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import gregor.avro.DeliveryAttempt;
import gregor.avro.DeliveryOutcome;
import gregor.avro.WebhookEvent;
import gregor.component.SignatureComponent;
import gregor.config.properties.WebhookProperties;
import gregor.route.WebhookRoute;
import gregor.service.WebhookService;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class CoreWebhookDeliveryLoopIntegrationTests {

    private static final String TENANT_ID = "00000000-0000-0000-0000-000000000001";
    private static final String ENDPOINT_ID = "00000000-0000-0000-0000-000000000002";
    private static final String SIGNING_SECRET = "whsec_" + Base64.getEncoder().encodeToString(
            "01234567890123456789012345678901".getBytes(StandardCharsets.UTF_8)
    );
    private static final TestReceiver RECEIVER = TestReceiver.start();

    @LocalServerPort
    private int serverPort;
    @Autowired
    private ProducerTemplate producerTemplate;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private SignatureComponent signatureComponent;
    @Autowired
    private WebhookService webhookService;
    @Autowired
    private WebhookProperties webhookProperties;

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
                KafkaConsumer<String, Object> eventConsumer =
                        this.newConsumer("integration-events-" + UUID.randomUUID());
                KafkaConsumer<String, Object> attemptConsumer =
                        this.newConsumer("integration-attempts-" + UUID.randomUUID())
        ) {
            this.seekToEnd(eventConsumer, "webhook-events");
            this.seekToEnd(attemptConsumer, "persist-delivery-attempt");

            HttpResponse<String> response = this.postEvent(
                    "{\"eventType\":\"order.created\",\"payload\":{\"orderId\":\"123\",\"name\":\"é\"}}"
            );

            Assertions.assertThat(response.statusCode()).isEqualTo(202);
            String eventId = this.objectMapper.readTree(response.body()).get("eventId").asString();
            Assertions.assertThatCode(() -> UUID.fromString(eventId)).doesNotThrowAnyException();

            ConsumerRecord<String, Object> eventRecord = this.awaitRecord(eventConsumer, eventId);
            Assertions.assertThat(eventRecord.key()).isEqualTo(ENDPOINT_ID);
            Assertions.assertThat(eventRecord.value()).isInstanceOf(WebhookEvent.class);
            WebhookEvent event = (WebhookEvent) eventRecord.value();
            Assertions.assertThat(event.getEventId()).isEqualTo(eventId);
            Assertions.assertThat(event.getEventType()).isEqualTo("order.created");
            Assertions.assertThat(event.getTenantId()).isEqualTo(TENANT_ID);
            Assertions.assertThat(event.getEndpointId()).isEqualTo(ENDPOINT_ID);
            Assertions.assertThat(event.getCreatedAt()).isBeforeOrEqualTo(Instant.now());
            Assertions.assertThat(event.getRetryCount()).isZero();
            Assertions.assertThat(bytes(event.getPayload()))
                    .isEqualTo("{\"orderId\":\"123\",\"name\":\"é\"}".getBytes(StandardCharsets.UTF_8));
            Assertions.assertThat(WebhookEvent.getClassSchema().getField("signingSecret")).isNull();

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

            ConsumerRecord<String, Object> attemptRecord = this.awaitRecord(attemptConsumer, eventId);
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
            Assertions.assertThat(DeliveryAttempt.getClassSchema().getField("payload")).isNull();
            Assertions.assertThat(DeliveryAttempt.getClassSchema().getField("signingSecret")).isNull();

            this.awaitAttemptRows(eventId, 1);
            Map<String, Object> row = this.jdbcTemplate.queryForMap(
                    """
                    SELECT tenant_id, endpoint_id, event_type, attempt_number, outcome, http_status,
                           latency_ms, attempted_at, details::text AS details
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
            Assertions.assertThat(row.get("details").toString()).isEqualTo("{}");
        }
    }

    @Test
    void acceptsEveryNonNullPayloadTypeWithoutDeduplicatingRepeatedRequests() throws Exception {
        List<String> payloads = List.of("{}", "[1]", "\"value\"", "42", "true");
        List<String> eventIds = new ArrayList<>();

        for (String payload : payloads) {
            HttpResponse<String> response = this.postEvent(
                    "{\"eventType\":\"payload.test\",\"payload\":" + payload + "}"
            );
            Assertions.assertThat(response.statusCode()).isEqualTo(202);
            eventIds.add(this.objectMapper.readTree(response.body()).get("eventId").asString());
        }

        HttpResponse<String> repeated = this.postEvent(
                "{\"eventType\":\"payload.test\",\"payload\":true}"
        );
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
                "{\"eventType\":\"\",\"payload\":{}}",
                "{\"eventType\":\"order.\\r\\ncreated\",\"payload\":{}}",
                "{\"eventType\":\"order.created\"}",
                "{\"eventType\":\"order.created\",\"payload\":null}"
        );
        try (KafkaConsumer<String, Object> consumer = this.newConsumer("integration-invalid-" + UUID.randomUUID())) {
            this.seekToEnd(consumer, "webhook-events");
            for (String body : invalidBodies) {
                HttpResponse<String> response = this.postEvent(body);
                Assertions.assertThat(response.statusCode()).isEqualTo(400);
                Assertions.assertThat(response.body())
                        .contains("\"title\":\"Invalid request\"")
                        .contains("\"detail\":\"Request body is malformed or invalid.\"");
            }
            Assertions.assertThat(consumer.poll(Duration.ofSeconds(1)).isEmpty()).isTrue();
        }
        Assertions.assertThat(RECEIVER.requestCount()).isZero();
    }

    @Test
    void retriesRetryableResponsesUntilTheConfiguredMaximumAndStopsPermanentResponsesAfterOneAttempt() throws Exception {
        int maximumAttempts = this.webhookProperties.maximumAttempts();
        List<Integer> expectedAttemptNumbers = IntStream.rangeClosed(1, maximumAttempts).boxed().toList();
        for (int status : List.of(408, 429, 503)) {
            RECEIVER.respondWith(status);
            HttpResponse<String> response = this.postEvent(
                    "{\"eventType\":\"retry.test\",\"payload\":{\"status\":" + status + "}}"
            );
            String eventId = this.objectMapper.readTree(response.body()).get("eventId").asString();

            RECEIVER.awaitRequest(eventId, maximumAttempts);
            this.awaitAttemptRows(eventId, maximumAttempts);
            List<Map<String, Object>> rows = this.jdbcTemplate.queryForList(
                    """
                    SELECT attempt_number, outcome, http_status, details::text AS details
                    FROM delivery_attempt
                    WHERE event_id = ?::uuid
                    ORDER BY attempt_number
                    """,
                    eventId
            );
            Assertions.assertThat(rows)
                    .extracting(row -> (Integer) row.get("attempt_number"))
                    .containsExactlyElementsOf(expectedAttemptNumbers);
            Assertions.assertThat(rows).allSatisfy(row -> {
                Assertions.assertThat(row.get("outcome")).isEqualTo("RETRYABLE_FAILURE");
                Assertions.assertThat(row.get("http_status")).isEqualTo(status);
                Assertions.assertThat(row.get("details").toString()).contains("HTTP_FAILURE");
            });
            Thread.sleep(200L);
            Assertions.assertThat(RECEIVER.count(eventId)).isEqualTo(maximumAttempts);
            RECEIVER.reset();
        }

        for (int status : List.of(302, 401)) {
            RECEIVER.respondWith(status);
            HttpResponse<String> response = this.postEvent(
                    "{\"eventType\":\"permanent.test\",\"payload\":{\"status\":" + status + "}}"
            );
            String eventId = this.objectMapper.readTree(response.body()).get("eventId").asString();

            RECEIVER.awaitRequest(eventId, 1);
            this.awaitAttemptRows(eventId, 1);
            Map<String, Object> row = this.jdbcTemplate.queryForMap(
                    """
                    SELECT attempt_number, outcome, http_status
                    FROM delivery_attempt
                    WHERE event_id = ?::uuid
                    """,
                    eventId
            );
            Assertions.assertThat(row)
                    .containsEntry("attempt_number", 1)
                    .containsEntry("outcome", "PERMANENT_FAILURE")
                    .containsEntry("http_status", status);
            Thread.sleep(200L);
            Assertions.assertThat(RECEIVER.count(eventId)).isEqualTo(1);
            RECEIVER.reset();
        }
    }

    @Test
    void retriesAbruptTransportFailuresUntilTheConfiguredMaximumWithSanitizedDetails() throws Exception {
        int maximumAttempts = this.webhookProperties.maximumAttempts();
        RECEIVER.closeConnectionsWithoutResponse();
        HttpResponse<String> response = this.postEvent("{\"eventType\":\"connection.test\",\"payload\":{}}");
        String eventId = this.objectMapper.readTree(response.body()).get("eventId").asString();

        RECEIVER.awaitRequest(eventId, maximumAttempts);
        this.awaitAttemptRows(eventId, maximumAttempts);
        List<String> detailsValues = this.jdbcTemplate.queryForList(
                "SELECT details::text FROM delivery_attempt WHERE event_id = ?::uuid ORDER BY attempt_number",
                String.class,
                eventId
        );
        Assertions.assertThat(detailsValues).hasSize(maximumAttempts).allSatisfy(detailsJson -> {
            JsonNode details = this.objectMapper.readTree(detailsJson);
            Assertions.assertThat(details.get("type").asString()).isIn("CONNECTION_FAILURE", "INVALID_RESPONSE");
            Assertions.assertThat(details.has("exceptionClass")).isTrue();
            Assertions.assertThat(details.has("message")).isTrue();
            Assertions.assertThat(detailsJson).doesNotContain(SIGNING_SECRET, "request-payload");
        });
    }

    @Test
    void boundsHttpFailureDetailsWithoutPersistingThePayloadOrSigningSecret() throws Exception {
        int maximumAttempts = this.webhookProperties.maximumAttempts();
        String responseBody = "a" + "🙂".repeat(1_100);
        RECEIVER.respondWith(503, responseBody);
        HttpResponse<String> response = this.postEvent(
                "{\"eventType\":\"details.test\",\"payload\":{\"private\":\"request-payload-marker\"}}"
        );
        String eventId = this.objectMapper.readTree(response.body()).get("eventId").asString();

        RECEIVER.awaitRequest(eventId, maximumAttempts);
        this.awaitAttemptRows(eventId, maximumAttempts);
        List<String> detailsValues = this.jdbcTemplate.queryForList(
                "SELECT details::text FROM delivery_attempt WHERE event_id = ?::uuid ORDER BY attempt_number",
                String.class,
                eventId
        );
        Assertions.assertThat(detailsValues).hasSize(maximumAttempts).allSatisfy(detailsJson -> {
            JsonNode details = this.objectMapper.readTree(detailsJson);
            String snippet = details.get("responseBody").asString();
            Assertions.assertThat(details.get("type").asString()).isEqualTo("HTTP_FAILURE");
            Assertions.assertThat(snippet.getBytes(StandardCharsets.UTF_8).length).isLessThanOrEqualTo(4_096);
            Assertions.assertThat(snippet).doesNotContain("�");
            Assertions.assertThat(responseBody).startsWith(snippet);
            Assertions.assertThat(detailsJson).doesNotContain("request-payload-marker", SIGNING_SECRET);
        });
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

        String detailsJson = this.jdbcTemplate.queryForObject(
                "SELECT details::text FROM delivery_attempt WHERE event_id = ?::uuid",
                String.class,
                eventId
        );
        Assertions.assertThat(this.objectMapper.readTree(detailsJson))
                .isEqualTo(this.objectMapper.readTree("{\"type\":\"HTTP_FAILURE\"}"));
        Assertions.assertThat(detailsJson).doesNotContain(payload, SIGNING_SECRET);
    }

    @Test
    void retriesTimedOutRequestsUntilTheConfiguredMaximumWithNullableStatus() throws Exception {
        int maximumAttempts = this.webhookProperties.maximumAttempts();
        RECEIVER.delayResponses(Duration.ofSeconds(6));
        HttpResponse<String> response = this.postEvent("{\"eventType\":\"timeout.test\",\"payload\":{}}");
        String eventId = this.objectMapper.readTree(response.body()).get("eventId").asString();

        RECEIVER.awaitRequest(eventId, maximumAttempts);
        this.awaitAttemptRows(eventId, maximumAttempts);
        List<Map<String, Object>> rows = this.jdbcTemplate.queryForList(
                """
                SELECT attempt_number, outcome, http_status, details::text AS details
                FROM delivery_attempt
                WHERE event_id = ?::uuid
                ORDER BY attempt_number
                """,
                eventId
        );
        Assertions.assertThat(rows).hasSize(maximumAttempts).allSatisfy(row -> {
            Assertions.assertThat(row.get("outcome")).isEqualTo("RETRYABLE_FAILURE");
            Assertions.assertThat(row.get("http_status")).isNull();
            Assertions.assertThat(row.get("details").toString()).contains("TIMEOUT");
        });
    }

    @Test
    void processesOnePartitionSequentiallyAndDifferentPartitionsConcurrently() throws Exception {
        this.awaitPartitionsAssignedToDifferentConsumers(
                WebhookRoute.WEBHOOK_EVENT_TOPIC,
                WebhookRoute.WEBHOOK_EVENT_TOPIC,
                0,
                1
        );
        RECEIVER.blockResponses();
        WebhookEvent first = event("same-partition-first", "endpoint-a");
        WebhookEvent second = event("same-partition-second", "endpoint-a");
        this.producerTemplate.sendBodyAndHeaders(
                "kafka:" + WebhookRoute.WEBHOOK_EVENT_TOPIC,
                first,
                Map.of(KafkaConstants.KEY, "endpoint-a", KafkaConstants.PARTITION_KEY, 0)
        );
        this.producerTemplate.sendBodyAndHeaders(
                "kafka:" + WebhookRoute.WEBHOOK_EVENT_TOPIC,
                second,
                Map.of(KafkaConstants.KEY, "endpoint-a", KafkaConstants.PARTITION_KEY, 0)
        );

        RECEIVER.awaitTotalRequests(1);
        Thread.sleep(300L);
        Assertions.assertThat(RECEIVER.requestCount()).isEqualTo(1);
        RECEIVER.releaseResponses();
        RECEIVER.awaitTotalRequests(2);
        this.awaitAttemptRows(first.getEventId(), 1);
        this.awaitAttemptRows(second.getEventId(), 1);

        RECEIVER.reset();
        RECEIVER.blockResponses();
        WebhookEvent partitionZero = event("different-partition-zero", "endpoint-zero");
        WebhookEvent partitionOne = event("different-partition-one", "endpoint-one");
        this.producerTemplate.sendBodyAndHeaders(
                "kafka:" + WebhookRoute.WEBHOOK_EVENT_TOPIC,
                partitionZero,
                Map.of(KafkaConstants.KEY, "endpoint-zero", KafkaConstants.PARTITION_KEY, 0)
        );
        this.producerTemplate.sendBodyAndHeaders(
                "kafka:" + WebhookRoute.WEBHOOK_EVENT_TOPIC,
                partitionOne,
                Map.of(KafkaConstants.KEY, "endpoint-one", KafkaConstants.PARTITION_KEY, 1)
        );

        RECEIVER.awaitRequests(partitionZero.getEventId(), partitionOne.getEventId());
        Assertions.assertThat(RECEIVER.requestCount()).isEqualTo(2);
        RECEIVER.releaseResponses();
        this.awaitAttemptRows(partitionZero.getEventId(), 1);
        this.awaitAttemptRows(partitionOne.getEventId(), 1);
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
                0
        );

        this.producerTemplate.sendBodyAndHeaders(
                "kafka:" + WebhookRoute.WEBHOOK_EVENT_TOPIC,
                event,
                Map.of(KafkaConstants.KEY, event.getEndpointId(), KafkaConstants.PARTITION_KEY, 0)
        );
        this.producerTemplate.sendBodyAndHeaders(
                "kafka:" + WebhookRoute.WEBHOOK_EVENT_TOPIC,
                event,
                Map.of(KafkaConstants.KEY, event.getEndpointId(), KafkaConstants.PARTITION_KEY, 0)
        );

        RECEIVER.awaitRequest(eventId, 2);
        this.awaitAttemptRows(eventId, 1);
        Thread.sleep(500L);
        Assertions.assertThat(RECEIVER.count(eventId)).isEqualTo(2);
        Assertions.assertThat(this.attemptRowCount(eventId)).isEqualTo(1);
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
                ByteBuffer.wrap("{\"type\":\"TIMEOUT\"}".getBytes(StandardCharsets.UTF_8))
        );

        Assertions.assertThatCode(() -> {
            this.webhookService.persistDeliveryAttempt(attempt);
            this.webhookService.persistDeliveryAttempt(attempt);
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
                SELECT attempt_id, event_id, attempt_number, outcome, http_status, latency_ms,
                       attempted_at, details::text AS details
                FROM delivery_attempt
                WHERE event_id = ?::uuid
                """,
                eventId
        );
        Assertions.assertThat(row.get("attempt_id").toString()).isEqualTo(attemptId);
        Assertions.assertThat(row.get("event_id").toString()).isEqualTo(eventId);
        Assertions.assertThat(row)
                .containsEntry("attempt_number", 3)
                .containsEntry("outcome", "RETRYABLE_FAILURE")
                .containsEntry("latency_ms", 42L);
        Assertions.assertThat(row.get("http_status")).isNull();
        Assertions.assertThat(row.get("details").toString()).contains("\"type\": \"TIMEOUT\"");
    }

    @Test
    void createsOnlyTheTwoPhaseOneTopicsWithConfiguredPartitionAndRetentionSettings() throws Exception {
        try (
                AdminClient adminClient = AdminClient.create(Map.of(
                        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                        kafkaBootstrapServers()
                ))
        ) {
            Assertions.assertThat(adminClient.listTopics().names().get(10, TimeUnit.SECONDS))
                    .containsExactlyInAnyOrder("webhook-events", "persist-delivery-attempt");
            Map<String, TopicDescription> descriptions = adminClient
                    .describeTopics(List.of("webhook-events", "persist-delivery-attempt"))
                    .allTopicNames()
                    .get(10, TimeUnit.SECONDS);
            Assertions.assertThat(descriptions).containsOnlyKeys("webhook-events", "persist-delivery-attempt");
            Assertions.assertThat(descriptions.values()).allSatisfy(description -> {
                Assertions.assertThat(description.partitions()).hasSize(12);
                Assertions.assertThat(description.partitions()).allSatisfy(partition ->
                        Assertions.assertThat(partition.replicas()).hasSize(1)
                );
            });

            ConfigResource webhookEvents = new ConfigResource(ConfigResource.Type.TOPIC, "webhook-events");
            ConfigResource deliveryAttempts = new ConfigResource(ConfigResource.Type.TOPIC, "persist-delivery-attempt");
            Map<ConfigResource, Config> configs = adminClient
                    .describeConfigs(List.of(webhookEvents, deliveryAttempts))
                    .all()
                    .get(10, TimeUnit.SECONDS);
            Assertions.assertThat(configValue(configs.get(webhookEvents), TopicConfig.CLEANUP_POLICY_CONFIG))
                    .isEqualTo(TopicConfig.CLEANUP_POLICY_DELETE);
            Assertions.assertThat(configValue(configs.get(webhookEvents), TopicConfig.RETENTION_MS_CONFIG))
                    .isEqualTo("604800000");
            Assertions.assertThat(configValue(configs.get(deliveryAttempts), TopicConfig.CLEANUP_POLICY_CONFIG))
                    .isEqualTo(TopicConfig.CLEANUP_POLICY_DELETE);
            Assertions.assertThat(configValue(configs.get(deliveryAttempts), TopicConfig.RETENTION_MS_CONFIG))
                    .isEqualTo("2592000000");
        }
    }

    @Test
    void localReceiverAcceptsOnlyValidCurrentStandardWebhookSignatures() throws Exception {
        byte[] body = "{\"receiver\":\"é\"}".getBytes(StandardCharsets.UTF_8);
        long now = Instant.now().getEpochSecond();
        String valid = this.signatureComponent.sign("receiver-event", now, body);

        Assertions.assertThat(this.postReceiver(body, "receiver-event", Long.toString(now), valid).statusCode())
                .isEqualTo(204);
        Assertions.assertThat(this.postReceiver(body, null, null, null).statusCode()).isEqualTo(401);
        Assertions.assertThat(this.postReceiver(body, "receiver-event", Long.toString(now), "v1,wrong").statusCode())
                .isEqualTo(401);

        long staleTimestamp = now - Duration.ofMinutes(5).toSeconds() - 1L;
        String stale = this.signatureComponent.sign("receiver-event", staleTimestamp, body);
        Assertions.assertThat(this.postReceiver(
                body,
                "receiver-event",
                Long.toString(staleTimestamp),
                stale
        ).statusCode()).isEqualTo(401);

        long futureTimestamp = Instant.now().getEpochSecond() + Duration.ofMinutes(6).toSeconds();
        String future = this.signatureComponent.sign("receiver-event", futureTimestamp, body);
        Assertions.assertThat(this.postReceiver(
                body,
                "receiver-event",
                Long.toString(futureTimestamp),
                future
        ).statusCode()).isEqualTo(401);
    }

    private HttpResponse<String> postEvent(String body) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + this.serverPort + "/api/events"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> postReceiver(
            byte[] body,
            String webhookId,
            String timestamp,
            String signature
    ) throws IOException, InterruptedException {
        HttpRequest.Builder request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + this.serverPort + "/api/mocked-webhook-receiver"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        if (webhookId != null) {
            request.header("webhook-id", webhookId);
        }
        if (timestamp != null) {
            request.header("webhook-timestamp", timestamp);
        }
        if (signature != null) {
            request.header("webhook-signature", signature);
        }
        return HttpClient.newHttpClient().send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private KafkaConsumer<String, Object> newConsumer(String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "io.apicurio.registry.serde.avro.AvroKafkaDeserializer"
        );
        properties.put(
                "apicurio.registry.url",
                "http://localhost:" + System.getenv().getOrDefault("SCHEMA_REGISTRY_PORT", "8081")
                        + "/apis/registry/v3"
        );
        properties.put("apicurio.registry.use-specific-avro-reader", true);
        return new KafkaConsumer<>(properties);
    }

    private void seekToEnd(KafkaConsumer<String, Object> consumer, String topic) {
        List<TopicPartition> partitions = consumer.partitionsFor(topic, Duration.ofSeconds(10)).stream()
                .map(PartitionInfo::partition)
                .map(partition -> new TopicPartition(topic, partition))
                .toList();
        consumer.assign(partitions);
        consumer.seekToEnd(partitions);
        for (TopicPartition partition : partitions) {
            consumer.position(partition);
        }
    }

    private ConsumerRecord<String, Object> awaitRecord(KafkaConsumer<String, Object> consumer, String eventId) {
        Instant deadline = Instant.now().plusSeconds(15);
        while (Instant.now().isBefore(deadline)) {
            for (ConsumerRecord<String, Object> record : consumer.poll(Duration.ofMillis(200))) {
                if (eventId.equals(eventId(record.value()))) {
                    return record;
                }
            }
        }
        throw new AssertionError("Kafka record was not observed for event " + eventId);
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
        try (
                AdminClient admin = AdminClient.create(Map.of(
                        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                        kafkaBootstrapServers()
                ))
        ) {
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

    private static byte[] bytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }

    private static String eventId(Object value) {
        if (value instanceof WebhookEvent event) {
            return event.getEventId();
        }
        if (value instanceof DeliveryAttempt attempt) {
            return attempt.getEventId();
        }
        return null;
    }

    private static WebhookEvent event(String eventType, String endpointId) {
        return new WebhookEvent(
                UUID.randomUUID().toString(),
                eventType,
                TENANT_ID,
                UUID.nameUUIDFromBytes(endpointId.getBytes(StandardCharsets.UTF_8)).toString(),
                Instant.now(),
                ByteBuffer.wrap(("{\"eventType\":\"" + eventType + "\"}").getBytes(StandardCharsets.UTF_8)),
                0
        );
    }

    private static String configValue(Config config, String key) {
        ConfigEntry entry = config.get(key);
        return entry == null ? null : entry.value();
    }

    private static String kafkaBootstrapServers() {
        return "localhost:" + System.getenv().getOrDefault("KAFKA_PORT", "9092");
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
        private volatile Duration delay = Duration.ZERO;
        private volatile byte[] responseBody = new byte[0];
        private volatile boolean closeWithoutResponse;
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

        void delayResponses(Duration responseDelay) {
            this.delay = responseDelay;
        }

        void closeConnectionsWithoutResponse() {
            this.closeWithoutResponse = true;
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
            this.delay = Duration.ZERO;
            this.responseBody = new byte[0];
            this.closeWithoutResponse = false;
            this.responseGate = null;
        }

        int requestCount() {
            return this.requests.size();
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

        void awaitTotalRequests(int expectedCount) throws InterruptedException {
            Instant deadline = Instant.now().plusSeconds(15);
            while (Instant.now().isBefore(deadline)) {
                if (this.requests.size() >= expectedCount) {
                    return;
                }
                Thread.sleep(25L);
            }
            throw new AssertionError("Expected " + expectedCount + " total requests");
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
            this.requests.add(new CapturedRequest(
                    body,
                    exchange.getRequestHeaders().getFirst("webhook-event-type"),
                    exchange.getRequestHeaders().getFirst("webhook-id"),
                    exchange.getRequestHeaders().getFirst("webhook-timestamp"),
                    exchange.getRequestHeaders().getFirst("webhook-signature"),
                    exchange.getRequestHeaders().getFirst("Content-Type")
            ));
            try (exchange) {
                if (this.closeWithoutResponse) {
                    return;
                }
                if (!this.delay.isZero()) {
                    Thread.sleep(this.delay);
                }
                CountDownLatch gate = this.responseGate;
                if (gate != null) {
                    gate.await(10, TimeUnit.SECONDS);
                }
                if (this.status.get() == 302) {
                    exchange.getResponseHeaders().add("Location", this.uri());
                }
                byte[] responseBytes = this.responseBody;
                exchange.sendResponseHeaders(this.status.get(), responseBytes.length == 0 ? -1L : responseBytes.length);
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
    }
}
