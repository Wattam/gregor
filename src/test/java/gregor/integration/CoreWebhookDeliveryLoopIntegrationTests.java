package gregor.integration;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import gregor.avro.DeliveryAttempt;
import gregor.avro.DeliveryOutcome;
import gregor.avro.WebhookEnvelopeCodec;
import gregor.avro.WebhookEnvelopeFixtures;
import gregor.avro.WebhookEvent;
import gregor.entity.DeadLetterOccurrence;
import gregor.repository.DeadLetterOccurrenceRepository;
import gregor.route.WebhookRoute;
import gregor.service.WebhookService;
import io.apicurio.registry.resolver.client.RegistryClientFacade;
import io.apicurio.registry.resolver.client.RegistryVersionCoordinates;
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import io.apicurio.registry.serde.kafka.config.KafkaSerdeConfig;
import io.vertx.core.Vertx;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericRecord;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.Route;
import org.apache.camel.component.kafka.DefaultKafkaClientFactory;
import org.apache.camel.component.kafka.KafkaClientFactory;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.apache.camel.spi.RouteController;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.env.Environment;
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
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {"spring.config.location=file:./src/main/resources/application.yaml", "gregor.webhook.maximum-attempts=4"})
class CoreWebhookDeliveryLoopIntegrationTests {

    private static final String TENANT_ID = "00000000-0000-0000-0000-000000000001";
    private static final String ENDPOINT_ID = "00000000-0000-0000-0000-000000000002";
    private static final String SIGNING_SECRET = "whsec_" + Base64.getEncoder().encodeToString("01234567890123456789012345678901".getBytes(StandardCharsets.UTF_8));
    private static final TestReceiver TEST_RECEIVER = TestReceiver.start();
    private static final String KAFKA_CLIENT_ID = "gregor-it-core-" + UUID.randomUUID();
    private static final String RETRY_TOPIC = WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC;

    @LocalServerPort
    private int serverPort;
    @Autowired
    private CamelContext camelContext;
    @Autowired
    @Qualifier("deadLetterKafkaClientFactory")
    private KafkaClientFactory deadLetterKafkaClientFactory;
    @Autowired
    private ProducerTemplate producerTemplate;
    @Autowired
    private Vertx registryVertx;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private Environment environment;
    @Autowired
    private WebhookEnvelopeCodec webhookEnvelopeCodec;
    @Autowired
    private DeadLetterOccurrenceRepository deadLetterOccurrenceRepository;
    @MockitoSpyBean
    private WebhookService webhookService;
    @MockitoSpyBean
    private Clock clock;

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
        dynamicPropertyRegistry.add("spring.kafka.bootstrap-servers", () -> "localhost:" + kafkaPort);
        dynamicPropertyRegistry.add("camel.component.kafka.brokers", () -> "localhost:" + kafkaPort);
        dynamicPropertyRegistry.add("camel.component.kafka.client-id", () -> KAFKA_CLIENT_ID);
        // Interrupt the pending wait before the first tier's real delay can elapse during shutdown.
        dynamicPropertyRegistry.add("camel.component.kafka.shutdown-timeout", () -> "1000");
        dynamicPropertyRegistry.add("camel.component.kafka.additional-properties[apicurio.registry.url]", () -> "http://localhost:" + registryPort + "/apis/registry/v3");
        dynamicPropertyRegistry.add("gregor.kafka.listener-concurrency", () -> "12");
        dynamicPropertyRegistry.add("gregor.webhook.tenant-id", () -> TENANT_ID);
        dynamicPropertyRegistry.add("gregor.webhook.endpoint-id", () -> ENDPOINT_ID);
        dynamicPropertyRegistry.add("gregor.webhook.receiver-url", TEST_RECEIVER::uri);
        dynamicPropertyRegistry.add("gregor.webhook.signing-secret", () -> SIGNING_SECRET);
        dynamicPropertyRegistry.add("gregor.webhook.connection-timeout", () -> "2s");
        dynamicPropertyRegistry.add("gregor.webhook.response-timeout", () -> "5s");
        dynamicPropertyRegistry.add("gregor.webhook.receiver-timestamp-tolerance", () -> "5m");
    }

    @BeforeEach
    void resetReceiver() {
        TEST_RECEIVER.reset();
    }

    @AfterAll
    static void stopReceiver() {
        TEST_RECEIVER.close();
    }

    @Test
    void acceptsAndDeliversAnEndpointKeyedSpecificAvroEventThenPersistsItsAttempt() throws Exception {
        Instant eventTime = Instant.ofEpochSecond(1_700_000_000L);
        Mockito.when(this.clock.instant()).thenReturn(eventTime);

        try (
                AdminClient adminClient = KafkaTestRecords.adminClient();
                KafkaConsumer<String, Object> eventKafkaConsumer = this.observeNewRecords(adminClient, WebhookRoute.WEBHOOK_EVENT_TOPIC);
                KafkaConsumer<String, Object> attemptKafkaConsumer = this.observeNewRecords(adminClient, WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC)
        ) {
            HttpResponse<String> httpResponse = this.postEvent("{\"eventType\":\"order.created\",\"payload\":{\"orderId\":\"123\",\"name\":\"é\"}}");

            Assertions.assertThat(httpResponse.statusCode()).isEqualTo(202);
            String eventId = this.objectMapper.readTree(httpResponse.body()).get("eventId").asString();
            Assertions.assertThatCode(() -> UUID.fromString(eventId)).doesNotThrowAnyException();

            ConsumerRecord<String, Object> eventConsumerRecord = KafkaTestRecords.awaitRecord(eventKafkaConsumer, eventId);
            Assertions.assertThat(eventConsumerRecord.key()).isEqualTo(ENDPOINT_ID);
            Assertions.assertThat(eventConsumerRecord.value()).isInstanceOf(WebhookEvent.class);
            WebhookEvent webhookEvent = (WebhookEvent) eventConsumerRecord.value();
            Assertions.assertThat(webhookEvent.getEventId()).isEqualTo(eventId);
            Assertions.assertThat(webhookEvent.getEventType()).isEqualTo("order.created");
            Assertions.assertThat(webhookEvent.getTenantId()).isEqualTo(TENANT_ID);
            Assertions.assertThat(webhookEvent.getEndpointId()).isEqualTo(ENDPOINT_ID);
            Assertions.assertThat(webhookEvent.getCreatedAt()).isEqualTo(eventTime);
            Assertions.assertThat(webhookEvent.getRetryCount()).isZero();
            Assertions.assertThat(webhookEvent.getNextEligibleAt()).isNull();
            Assertions.assertThat(webhookEvent.getDeliveryAttemptCount()).isZero();
            Assertions.assertThat(webhookEvent.getReplayCount()).isZero();
            Assertions.assertThat(bytes(webhookEvent.getPayload())).isEqualTo("{\"orderId\":\"123\",\"name\":\"é\"}".getBytes(StandardCharsets.UTF_8));

            CapturedRequest initialCapturedRequest = TEST_RECEIVER.awaitRequest(eventId, 1);
            Assertions.assertThat(initialCapturedRequest.webhookId()).isEqualTo(eventId);
            Assertions.assertThat(initialCapturedRequest.bodyBytes()).isEqualTo("{\"orderId\":\"123\",\"name\":\"é\"}".getBytes(StandardCharsets.UTF_8));
            Assertions.assertThat(initialCapturedRequest.eventType()).isEqualTo("order.created");
            Assertions.assertThat(initialCapturedRequest.timestamp()).isEqualTo("1700000000");
            Assertions.assertThat(initialCapturedRequest.contentType()).startsWith("application/json");
            Assertions.assertThat(initialCapturedRequest.signature())
                    .isEqualTo(WebhookSignatureFixtures.expectedSignature(SIGNING_SECRET, eventId, initialCapturedRequest.timestamp(), "{\"orderId\":\"123\",\"name\":\"é\"}".getBytes(StandardCharsets.UTF_8)));

            ConsumerRecord<String, Object> attemptConsumerRecord = KafkaTestRecords.awaitRecord(attemptKafkaConsumer, eventId);
            Assertions.assertThat(attemptConsumerRecord.key()).isEqualTo(eventId);
            Assertions.assertThat(attemptConsumerRecord.value()).isInstanceOf(DeliveryAttempt.class);
            DeliveryAttempt deliveryAttempt = (DeliveryAttempt) attemptConsumerRecord.value();
            Assertions.assertThatCode(() -> UUID.fromString(deliveryAttempt.getAttemptId())).doesNotThrowAnyException();
            Assertions.assertThat(deliveryAttempt.getEventId()).isEqualTo(eventId);
            Assertions.assertThat(deliveryAttempt.getAttemptNumber()).isEqualTo(1);
            Assertions.assertThat(deliveryAttempt.getOutcome()).isEqualTo(DeliveryOutcome.SUCCEEDED);
            Assertions.assertThat(deliveryAttempt.getHttpStatus()).isEqualTo(204);
            Assertions.assertThat(deliveryAttempt.getLatencyMs()).isNotNegative();
            Assertions.assertThat(deliveryAttempt.getAttemptedAt()).isEqualTo(eventTime);
            Assertions.assertThat(deliveryAttempt.getRetryCount()).isZero();
            Assertions.assertThat(deliveryAttempt.getReplayCount()).isZero();
            Assertions.assertThat(deliveryAttempt.getDeadLettered()).isFalse();

            this.awaitAttemptRows(eventId, 1);
            Map<String, Object> attemptColumns = this.jdbcTemplate.queryForMap(
                    """
                    SELECT tenant_id, endpoint_id, event_type, attempt_number, outcome, http_status,
                           latency_ms, attempted_at, retry_count, replay_count, dead_lettered,
                           details::text AS details
                    FROM delivery_attempt
                    WHERE event_id = ?::uuid
                    """,
                    eventId
            );
            Assertions.assertThat(attemptColumns.get("tenant_id").toString()).isEqualTo(TENANT_ID);
            Assertions.assertThat(attemptColumns.get("endpoint_id").toString()).isEqualTo(ENDPOINT_ID);
            Assertions.assertThat(attemptColumns).containsEntry("event_type", "order.created").containsEntry("attempt_number", 1).containsEntry("outcome", "SUCCEEDED").containsEntry("http_status", 204);
            Assertions.assertThat((Long) attemptColumns.get("latency_ms")).isNotNegative();
            assertPhaseOneAuditFields(attemptColumns);
            Assertions.assertThat(attemptColumns.get("details").toString()).isEqualTo("{}");
        }
    }

    @Test
    void deliversRepeatedIngestRequestsAsDistinctEvents() throws Exception {
        String body = "{\"eventType\":\"payload.test\",\"payload\":true}";
        HttpResponse<String> initialHttpResponse = this.postEvent(body);
        HttpResponse<String> repeatedHttpResponse = this.postEvent(body);

        Assertions.assertThat(initialHttpResponse.statusCode()).isEqualTo(202);
        Assertions.assertThat(repeatedHttpResponse.statusCode()).isEqualTo(202);
        String initialEventId = this.objectMapper.readTree(initialHttpResponse.body()).get("eventId").asString();
        String repeatedEventId = this.objectMapper.readTree(repeatedHttpResponse.body()).get("eventId").asString();
        Assertions.assertThat(initialEventId).isNotEqualTo(repeatedEventId);
        for (String eventId : List.of(initialEventId, repeatedEventId)) {
            TEST_RECEIVER.awaitRequest(eventId, 1);
            this.awaitAttemptRows(eventId, 1);
        }
    }

    @Test
    void rejectsAnInvalidIngestRequestWithoutPublishingAnEvent() throws Exception {
        String topic = WebhookRoute.WEBHOOK_EVENT_TOPIC;
        try (AdminClient adminClient = KafkaTestRecords.adminClient()) {
            Map<TopicPartition, Long> offsetsBefore = KafkaTestRecords.endOffsets(adminClient, topic);
            HttpResponse<String> httpResponse = this.postEvent("{\"eventType\":\"order.created\",\"payload\":null}");

            Assertions.assertThat(httpResponse.statusCode()).isEqualTo(400);
            Assertions.assertThat(KafkaTestRecords.endOffsets(adminClient, topic)).isEqualTo(offsetsBefore);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {204, 400})
    void deliversALaterSourceEventBeforeThePendingRetryAndResumesTheRetryAfterRestart(int retryHttpStatus) throws Exception {
        Instant initial = Instant.parse("2026-07-25T12:00:00.123Z");
        Instant due = Instant.parse("2026-07-25T12:01:00.123Z");
        AtomicReference<Instant> currentInstantReference = new AtomicReference<>(initial);
        Mockito.doAnswer(_ -> currentInstantReference.get()).when(this.clock).instant();
        WebhookEvent firstWebhookEvent = event("delayed-first.test", ENDPOINT_ID);
        WebhookEvent laterWebhookEvent = event("delayed-later.test", ENDPOINT_ID);
        String eventId = firstWebhookEvent.getEventId();
        CountDownLatch eligibilityStartedCountDownLatch = new CountDownLatch(1);
        Mockito.doAnswer(invocationOnMock -> {
                    eligibilityStartedCountDownLatch.countDown();
                    return invocationOnMock.callRealMethod();
                })
                .when(this.webhookService)
                .awaitEligibility(Mockito.argThat(webhookRouteMessage -> Objects.equals(eventId, webhookRouteMessage.webhookEvent().getEventId())));
        String retryRoute = KafkaTestRecords.consumerRoute(RETRY_TOPIC);
        RouteController routeController = this.camelContext.getRouteController();
        String sourceTopic = WebhookRoute.WEBHOOK_EVENT_TOPIC;
        try (AdminClient adminClient = KafkaTestRecords.adminClient()) {
            Map<TopicPartition, Long> sourceOffsets = KafkaTestRecords.endOffsets(adminClient, sourceTopic);
            Map<TopicPartition, Long> retryOffsets = KafkaTestRecords.retryEndOffsets(adminClient);
            try (
                    AutoCloseable _ = () -> {
                        TEST_RECEIVER.clearRetryableFailure();
                        TEST_RECEIVER.respondWith(204);
                        try {
                            KafkaTestRecords.awaitPublishedCommits(adminClient, sourceTopic, sourceOffsets);
                        } finally {
                            KafkaTestRecords.drainRetries(adminClient, routeController, retryOffsets, currentInstantReference);
                            if (routeController.getRouteStatus(retryRoute).isStopped()) {
                                routeController.startRoute(retryRoute);
                            }
                        }
                    };
                    KafkaConsumer<String, Object> sourceKafkaConsumer = KafkaTestRecords.consumer(this.registryVertx, sourceOffsets);
                    KafkaConsumer<String, Object> retryKafkaConsumer = KafkaTestRecords.consumer(this.registryVertx, retryOffsets);
                    KafkaConsumer<String, Object> deadLetterKafkaConsumer = this.observeNewRecords(adminClient, "webhook-events-dead-letter")
            ) {
                TEST_RECEIVER.failOnly(eventId);
                this.sendToPartition(firstWebhookEvent, 0);
                KafkaTestRecords.awaitCommit(adminClient, KafkaTestRecords.awaitRecord(sourceKafkaConsumer, eventId));
                ConsumerRecord<String, Object> retryConsumerRecord = KafkaTestRecords.awaitRecord(retryKafkaConsumer, eventId);
                Assertions.assertThat(retryConsumerRecord.topic()).isEqualTo(RETRY_TOPIC);
                WebhookEvent retryWebhookEvent = (WebhookEvent) retryConsumerRecord.value();
                Assertions.assertThat(retryConsumerRecord.key()).isEqualTo(firstWebhookEvent.getEndpointId());
                Assertions.assertThat(retryWebhookEvent.getNextEligibleAt()).isEqualTo(due);
                Assertions.assertThat(eligibilityStartedCountDownLatch.await(15, TimeUnit.SECONDS)).isTrue();

                this.sendToPartition(laterWebhookEvent, 0);
                KafkaTestRecords.awaitCommit(adminClient, KafkaTestRecords.awaitRecord(sourceKafkaConsumer, laterWebhookEvent.getEventId()));
                TEST_RECEIVER.awaitRequest(laterWebhookEvent.getEventId(), 1);
                this.awaitAttemptRows(laterWebhookEvent.getEventId(), 1);
                Assertions.assertThat(this.jdbcTemplate.queryForObject("SELECT outcome FROM delivery_attempt WHERE event_id = ?::uuid", String.class, laterWebhookEvent.getEventId())).isEqualTo("SUCCEEDED");
                Assertions.assertThat(currentInstantReference.get()).isBefore(due);
                Assertions.assertThat(TEST_RECEIVER.count(eventId)).isEqualTo(1);
                this.awaitAttemptRows(eventId, 1);

                KafkaTestRecords.stopRetryConsumer(routeController, retryRoute);
                Assertions.assertThat(TEST_RECEIVER.count(eventId)).isEqualTo(1);
                TEST_RECEIVER.clearRetryableFailure();
                TEST_RECEIVER.respondWith(retryHttpStatus);
                currentInstantReference.set(due);
                routeController.startRoute(retryRoute);
                CapturedRequest retriedCapturedRequest = TEST_RECEIVER.awaitRequest(eventId, 2);
                KafkaTestRecords.awaitCommit(adminClient, retryConsumerRecord);
                this.awaitAttemptRows(eventId, 2);
                Assertions.assertThat(TEST_RECEIVER.count(eventId)).isEqualTo(2);
                Assertions.assertThat(retriedCapturedRequest.webhookId()).isEqualTo(eventId);
                Assertions.assertThat(retriedCapturedRequest.timestamp()).isEqualTo(Long.toString(due.getEpochSecond()));
                Assertions.assertThat(retriedCapturedRequest.bodyBytes()).isEqualTo(bytes(firstWebhookEvent.getPayload()));
                boolean terminal = retryHttpStatus == 400;
                Assertions.assertThat(this.jdbcTemplate.queryForList(
                                """
                                SELECT attempt_number, retry_count, replay_count, outcome, http_status, dead_lettered
                                FROM delivery_attempt WHERE event_id = ?::uuid ORDER BY attempt_number
                                """,
                                eventId
                        ))
                        .containsExactly(
                                Map.of("attempt_number", 1, "retry_count", 0, "replay_count", 0, "outcome", "RETRYABLE_FAILURE", "http_status", 503, "dead_lettered", false),
                                Map.of("attempt_number", 2, "retry_count", 1, "replay_count", 0, "outcome", terminal ? "PERMANENT_FAILURE" : "SUCCEEDED", "http_status", retryHttpStatus, "dead_lettered", terminal)
                        );
                if (terminal) {
                    ConsumerRecord<String, Object> deadLetterConsumerRecord = KafkaTestRecords.awaitRecord(deadLetterKafkaConsumer, eventId);
                    Assertions.assertThat(deadLetterConsumerRecord.key()).isEqualTo(firstWebhookEvent.getEndpointId());
                    Assertions.assertThat(deadLetterConsumerRecord.value()).isEqualTo(WebhookEvent.newBuilder(retryWebhookEvent).setDeliveryAttemptCount(2).build());
                }
            }
        }
    }

    @Test
    void deadLettersRedirectsWithoutFollowingThem() throws Exception {
        TEST_RECEIVER.respondWith(302);
        try (
                AdminClient adminClient = KafkaTestRecords.adminClient();
                KafkaConsumer<String, Object> sourceKafkaConsumer = this.observeNewRecords(adminClient, WebhookRoute.WEBHOOK_EVENT_TOPIC);
                KafkaConsumer<String, Object> deadLetterKafkaConsumer = this.observeNewRecords(adminClient, "webhook-events-dead-letter")
        ) {
            HttpResponse<String> httpResponse = this.postEvent("{\"eventType\":\"redirect.test\",\"payload\":{}}");
            Assertions.assertThat(httpResponse.statusCode()).isEqualTo(202);
            String eventId = this.objectMapper.readTree(httpResponse.body()).get("eventId").asString();

            ConsumerRecord<String, Object> inputConsumerRecord = KafkaTestRecords.awaitRecord(sourceKafkaConsumer, eventId);
            KafkaTestRecords.awaitCommit(adminClient, inputConsumerRecord);
            ConsumerRecord<String, Object> deadLetterConsumerRecord = KafkaTestRecords.awaitRecord(deadLetterKafkaConsumer, eventId);
            Assertions.assertThat(deadLetterConsumerRecord.key()).isEqualTo(ENDPOINT_ID);
            Assertions.assertThat(deadLetterConsumerRecord.value()).isEqualTo(WebhookEvent.newBuilder((WebhookEvent) inputConsumerRecord.value()).setDeliveryAttemptCount(1).build());
            this.awaitAttemptRows(eventId, 1);
            Assertions.assertThat(TEST_RECEIVER.count(eventId)).isEqualTo(1);
            Assertions.assertThat(this.jdbcTemplate.queryForMap("SELECT outcome, http_status, dead_lettered FROM delivery_attempt WHERE event_id = ?::uuid", eventId))
                    .containsEntry("outcome", "PERMANENT_FAILURE")
                    .containsEntry("http_status", 302)
                    .containsEntry("dead_lettered", true);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void deadLettersTransportFailuresAndAuditsNullHttpStatusWhenNoHigherTierExists(boolean timeout) throws Exception {
        WebhookEvent webhookEvent = WebhookEvent.newBuilder(event("transport.test", ENDPOINT_ID)).setCreatedAt(Instant.parse("2026-07-25T11:55:00Z")).setRetryCount(3).setDeliveryAttemptCount(2).setNextEligibleAt(Instant.EPOCH).build();
        WebhookEvent unrelatedWebhookEvent = WebhookEvent.newBuilder(webhookEvent).setEventId(UUID.randomUUID().toString()).build();
        String topic = WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC;
        TEST_RECEIVER.failTransportOnly(webhookEvent.getEventId(), timeout);
        try (
                AdminClient adminClient = KafkaTestRecords.adminClient();
                KafkaConsumer<String, Object> inputKafkaConsumer = this.observeNewRecords(adminClient, topic);
                KafkaConsumer<String, Object> deadLetterKafkaConsumer = this.observeNewRecords(adminClient, "webhook-events-dead-letter")
        ) {
            for (WebhookEvent inputWebhookEvent : List.of(unrelatedWebhookEvent, webhookEvent)) {
                this.producerTemplate.sendBodyAndHeader("kafka:" + topic, inputWebhookEvent, KafkaConstants.KEY, inputWebhookEvent.getEndpointId());
                KafkaTestRecords.awaitCommit(adminClient, KafkaTestRecords.awaitRecord(inputKafkaConsumer, inputWebhookEvent.getEventId()));
            }
            ConsumerRecord<String, Object> deadLetterConsumerRecord = KafkaTestRecords.awaitRecord(deadLetterKafkaConsumer, webhookEvent.getEventId());
            Assertions.assertThat(deadLetterConsumerRecord.key()).isEqualTo(webhookEvent.getEndpointId());
            Assertions.assertThat(deadLetterConsumerRecord.value()).isEqualTo(WebhookEvent.newBuilder(webhookEvent).setDeliveryAttemptCount(3).build());
        } finally {
            TEST_RECEIVER.clearTransportFailure();
        }
        this.awaitAttemptRows(unrelatedWebhookEvent.getEventId(), 1);
        Assertions.assertThat(this.jdbcTemplate.queryForObject("SELECT outcome FROM delivery_attempt WHERE event_id = ?::uuid", String.class, unrelatedWebhookEvent.getEventId())).isEqualTo("SUCCEEDED");
        this.awaitAttemptRows(webhookEvent.getEventId(), 1);
        Assertions.assertThat(TEST_RECEIVER.count(webhookEvent.getEventId())).isEqualTo(1);
        Map<String, Object> attemptColumns = this.jdbcTemplate.queryForMap(
                """
                SELECT attempt_number, outcome, http_status, retry_count, replay_count, dead_lettered,
                       details::text AS details
                FROM delivery_attempt WHERE event_id = ?::uuid
                """,
                webhookEvent.getEventId()
        );
        Assertions.assertThat(attemptColumns)
                .containsEntry("attempt_number", 3)
                .containsEntry("outcome", "RETRYABLE_FAILURE")
                .containsEntry("http_status", null)
                .containsEntry("retry_count", 3)
                .containsEntry("replay_count", 0)
                .containsEntry("dead_lettered", true);
        JsonNode detailsJsonNode = this.objectMapper.readTree(attemptColumns.get("details").toString());
        Assertions.assertThat(detailsJsonNode.get("type").asString()).isIn(timeout ? List.of("TIMEOUT") : List.of("CONNECTION_FAILURE", "INVALID_RESPONSE"));
        Assertions.assertThat(detailsJsonNode.has("exceptionClass")).isTrue();
        Assertions.assertThat(detailsJsonNode.has("message")).isTrue();
    }

    @Test
    void persistsTerminalOccurrenceIndependentlyOfDeliveryAndAuditCommits() throws Exception {
        String eventId = UUID.randomUUID().toString();
        String payload = "{\"private\":\"request-payload-marker\"}";
        Instant httpTime = Instant.parse("2001-01-02T03:04:05Z");
        TEST_RECEIVER.respondWith(400, "Unknown customer: request-payload-marker");
        Mockito.when(this.clock.instant()).thenReturn(httpTime);
        WebhookEvent webhookEvent = WebhookEvent
                .newBuilder(event("sensitive-details.test", ENDPOINT_ID))
                .setEventId(eventId)
                .setCreatedAt(httpTime.minusSeconds(600))
                .setNextEligibleAt(httpTime.minusSeconds(60))
                .setPayload(ByteBuffer.wrap(payload.getBytes(StandardCharsets.UTF_8)))
                .build();
        CountDownLatch terminalDeliveryCountDownLatch = new CountDownLatch(1);
        Mockito.doAnswer(invocationOnMock -> {
                    invocationOnMock.callRealMethod();
                    if (!terminalDeliveryCountDownLatch.await(30, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Fixture did not release terminal delivery");
                    }
                    return null;
                })
                .when(this.webhookService)
                .publishDeadLetter(Mockito.argThat(webhookRouteMessage -> Objects.equals(webhookRouteMessage.webhookEvent().getEventId(), eventId)));

        RouteController routeController = this.camelContext.getRouteController();
        routeController.stopRoute(DeadLetterTestRecords.ROUTE_ID);
        try (
                AdminClient adminClient = KafkaTestRecords.adminClient();
                KafkaConsumer<String, Object> sourceKafkaConsumer = this.observeNewRecords(adminClient, WebhookRoute.WEBHOOK_EVENT_TOPIC);
                KafkaConsumer<String, Object> attemptKafkaConsumer = this.observeNewRecords(adminClient, WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC);
                KafkaConsumer<String, Object> deadLetterKafkaConsumer = this.observeNewRecords(adminClient, DeadLetterTestRecords.GROUP_ID)
        ) {
            ConsumerRecord<String, Object> deadLetterConsumerRecord;
            UUID eventUuid = UUID.fromString(eventId);
            try (DeadLetterDatabaseFixtures.InsertGate insertGate = DeadLetterDatabaseFixtures.holdInsert(this.jdbcTemplate, eventUuid)) {
                this.sendToPartition(webhookEvent, 0);
                deadLetterConsumerRecord = KafkaTestRecords.awaitRecord(deadLetterKafkaConsumer, eventId);
                ConsumerRecord<String, Object> sourceConsumerRecord = KafkaTestRecords.awaitRecord(sourceKafkaConsumer, eventId);
                Instant recordTime = Instant.ofEpochMilli(deadLetterConsumerRecord.timestamp());
                Assertions.assertThat(recordTime).isNotIn(webhookEvent.getCreatedAt(), webhookEvent.getNextEligibleAt(), httpTime);
                awaitClockPast(recordTime);
                routeController.startRoute(DeadLetterTestRecords.ROUTE_ID);
                insertGate.awaitPendingInsert();

                Assertions.assertThat(KafkaTestRecords.committedOffset(adminClient, sourceConsumerRecord)).isLessThanOrEqualTo(sourceConsumerRecord.offset());
                Assertions.assertThat(this.attemptRowCount(eventId)).isZero();
                terminalDeliveryCountDownLatch.countDown();
                KafkaTestRecords.awaitCommit(adminClient, sourceConsumerRecord);
                ConsumerRecord<String, Object> attemptConsumerRecord = KafkaTestRecords.awaitRecord(attemptKafkaConsumer, eventId);
                KafkaTestRecords.awaitCommit(adminClient, attemptConsumerRecord);
                Assertions.assertThat(((DeliveryAttempt) attemptConsumerRecord.value()).getAttemptedAt()).isEqualTo(httpTime);
                Assertions.assertThat(this.attemptRowCount(eventId)).isEqualTo(1);
                Assertions.assertThat(TEST_RECEIVER.awaitRequest(eventId, 1).bodyBytes()).isEqualTo(payload.getBytes(StandardCharsets.UTF_8));
                this.assertTerminalAuditWithoutReceiverResponse(eventId);
                Assertions.assertThat(KafkaTestRecords.committedOffset(adminClient, deadLetterConsumerRecord)).isLessThanOrEqualTo(deadLetterConsumerRecord.offset());
                Assertions.assertThat(this.deadLetterOccurrenceRepository.findByEventIdAndReplayCount(eventUuid, 0)).isEmpty();
            }

            KafkaTestRecords.awaitCommit(adminClient, deadLetterConsumerRecord);
            Assertions.assertThat(KafkaTestRecords.committedOffset(adminClient, deadLetterConsumerRecord)).isEqualTo(deadLetterConsumerRecord.offset() + 1);
            DeadLetterOccurrence deadLetterOccurrence = this.assertOccurrence(deadLetterConsumerRecord, eventUuid);
            Assertions.assertThat(this.webhookEnvelopeCodec.decode(deadLetterOccurrence.getStoredWebhookEnvelope())).isEqualTo(WebhookEvent.newBuilder(webhookEvent).setDeliveryAttemptCount(1).build());
            Assertions.assertThat(TEST_RECEIVER.count(eventId)).isEqualTo(1);
        } finally {
            terminalDeliveryCountDownLatch.countDown();
            if (routeController.getRouteStatus(DeadLetterTestRecords.ROUTE_ID).isStopped()) {
                routeController.startRoute(DeadLetterTestRecords.ROUTE_ID);
            }
        }
    }

    @Test
    void storesTerminalInputWithoutDeliveryChecksOrSideEffects() throws Exception {
        Instant recordTime = Instant.now().minus(Duration.ofHours(1)).truncatedTo(ChronoUnit.MILLIS);
        List<TerminalInput> terminalInputs = List.of(
                new TerminalInput(1, 0, recordTime.minusSeconds(1)),
                new TerminalInput(4, 3, recordTime.minusSeconds(1)),
                new TerminalInput(4, 3, recordTime.plus(Duration.ofDays(2))),
                new TerminalInput(4, 3, null),
                new TerminalInput(5, 4, null),
                new TerminalInput(-1, 3, null),
                new TerminalInput(4, -1, null)
        );
        RegistryClientFacade registryClientFacade = DeadLetterTestRecords.registry(this.environment, this.registryVertx);
        RegistryVersionCoordinates coordinates = DeadLetterTestRecords.registerCurrent(registryClientFacade);
        String[] topics = DeadLetterTestRecords.APPLICATION_TOPICS.toArray(String[]::new);
        List<UUID> occurrenceIds = new ArrayList<>();

        try (AdminClient adminClient = KafkaTestRecords.adminClient(); KafkaProducer<String, byte[]> kafkaProducer = KafkaTestRecords.rawProducer()) {
            Map<TopicPartition, Long> offsetsBefore = KafkaTestRecords.endOffsets(adminClient, topics);
            Map<TopicPartition, Long> expectedOffsets = new HashMap<>(offsetsBefore);
            TopicPartition topicPartition = new TopicPartition(DeadLetterTestRecords.GROUP_ID, 0);
            try (KafkaConsumer<String, Object> kafkaConsumer = KafkaTestRecords.consumer(this.registryVertx, offsetsBefore)) {
                for (TerminalInput terminalInput : terminalInputs) {
                    UUID eventId = UUID.fromString("abcdefab-" + UUID.randomUUID().toString().substring(9));
                    String uppercaseEventId = eventId.toString().toUpperCase(Locale.ROOT);
                    GenericRecord genericRecord = DeadLetterTestRecords.terminalDatum(uppercaseEventId, 0, terminalInput.nextEligibleAt());
                    genericRecord.put("eventType", "direct-storage.test");
                    genericRecord.put("deliveryAttemptCount", terminalInput.deliveryAttemptCount());
                    genericRecord.put("retryCount", terminalInput.retryCount());
                    byte[] datumBytes = WebhookEnvelopeFixtures.writeDatum(genericRecord);
                    Headers headers = new RecordHeaders().add("signing-secret", SIGNING_SECRET.getBytes(StandardCharsets.UTF_8));
                    RecordMetadata recordMetadata = DeadLetterTestRecords.send(kafkaProducer, topicPartition, recordTime, ENDPOINT_ID, DeadLetterTestRecords.prefix(coordinates, datumBytes), headers);
                    expectedOffsets.compute(topicPartition, (_, offset) -> offset + 1);
                    ConsumerRecord<String, Object> consumerRecord = KafkaTestRecords.awaitRecord(kafkaConsumer, uppercaseEventId);
                    Assertions.assertThat(consumerRecord.topic()).isEqualTo(DeadLetterTestRecords.GROUP_ID);
                    Assertions.assertThat(consumerRecord.partition()).isEqualTo(topicPartition.partition());
                    Assertions.assertThat(consumerRecord.offset()).isEqualTo(recordMetadata.offset());
                    Assertions.assertThat(consumerRecord.timestamp()).isEqualTo(recordTime.toEpochMilli());
                    KafkaTestRecords.awaitCommit(adminClient, consumerRecord);
                    Assertions.assertThat(KafkaTestRecords.committedOffset(adminClient, consumerRecord)).isEqualTo(recordMetadata.offset() + 1);

                    DeadLetterOccurrence deadLetterOccurrence = this.assertOccurrence(consumerRecord, eventId);
                    occurrenceIds.add(deadLetterOccurrence.getOccurrenceId());
                    Assertions.assertThat(deadLetterOccurrence.getStoredWebhookEnvelope().datumBytes()).containsExactly(datumBytes);
                    Assertions.assertThat(deadLetterOccurrence.getStoredWebhookEnvelope().writerSchemaJson()).doesNotContain(SIGNING_SECRET, "signing-secret");
                    WebhookEvent storedWebhookEvent = this.webhookEnvelopeCodec.decode(deadLetterOccurrence.getStoredWebhookEnvelope());
                    Assertions.assertThat(storedWebhookEvent.getDeliveryAttemptCount()).isEqualTo(terminalInput.deliveryAttemptCount());
                    Assertions.assertThat(storedWebhookEvent.getRetryCount()).isEqualTo(terminalInput.retryCount());
                    Assertions.assertThat(storedWebhookEvent.getNextEligibleAt()).isEqualTo(terminalInput.nextEligibleAt());
                    Assertions.assertThat(TEST_RECEIVER.count(uppercaseEventId)).isZero();
                    Assertions.assertThat(TEST_RECEIVER.count(eventId.toString())).isZero();
                    Assertions.assertThat(this.attemptRowCount(eventId.toString())).isZero();
                }
            }
            Assertions.assertThat(KafkaTestRecords.endOffsets(adminClient, topics)).isEqualTo(expectedOffsets);
        }
        Assertions.assertThat(occurrenceIds).hasSize(7).doesNotHaveDuplicates();
    }

    private DeadLetterOccurrence assertOccurrence(ConsumerRecord<String, Object> consumerRecord, UUID eventId) {
        DeadLetterOccurrence deadLetterOccurrence = this.deadLetterOccurrenceRepository.findByEventIdAndReplayCount(eventId, 0).orElseThrow();
        Assertions.assertThat(deadLetterOccurrence.getOccurrenceId()).isNotNull().isNotEqualTo(eventId);
        Assertions.assertThat(deadLetterOccurrence.getEventId()).isEqualTo(eventId);
        Assertions.assertThat(deadLetterOccurrence.getReplayCount()).isZero();
        Assertions.assertThat(deadLetterOccurrence.isReplayAccepted()).isFalse();
        Assertions.assertThat(deadLetterOccurrence.getDeadLetteredAt()).isEqualTo(Instant.ofEpochMilli(consumerRecord.timestamp()));
        Assertions.assertThat(this.jdbcTemplate.queryForObject("SELECT count(*) FROM dead_letter_occurrence WHERE event_id = ?", Integer.class, eventId)).isEqualTo(1);
        return deadLetterOccurrence;
    }

    private static void awaitClockPast(Instant recordTime) throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(5);
        while (Instant.now().isBefore(deadline)) {
            if (Instant.now().truncatedTo(ChronoUnit.MILLIS).isAfter(recordTime)) {
                return;
            }
            Thread.sleep(10L);
        }
        throw new AssertionError("The consumption time must differ from the terminal Kafka timestamp");
    }

    private void assertTerminalAuditWithoutReceiverResponse(String eventId) {
        Map<String, Object> attemptColumns = this.jdbcTemplate.queryForMap(
                """
                SELECT retry_count, replay_count, dead_lettered, outcome, http_status, details::text AS details
                FROM delivery_attempt
                WHERE event_id = ?::uuid
                """,
                eventId
        );
        Assertions.assertThat(attemptColumns).containsEntry("retry_count", 0).containsEntry("replay_count", 0).containsEntry("dead_lettered", true).containsEntry("outcome", "PERMANENT_FAILURE").containsEntry("http_status", 400);
        Assertions.assertThat(this.objectMapper.readTree(attemptColumns.get("details").toString())).isEqualTo(this.objectMapper.readTree("{\"type\":\"HTTP_FAILURE\"}"));
    }

    @Test
    void processesOnePartitionSequentiallyAndDifferentPartitionsConcurrently() throws Exception {
        this.awaitPartitionsAssignedToDifferentConsumers(WebhookRoute.WEBHOOK_EVENT_TOPIC, WebhookRoute.WEBHOOK_EVENT_TOPIC, 0, 1);
        WebhookEvent firstWebhookEvent = event("same-partition-first", "endpoint-a");
        WebhookEvent secondWebhookEvent = event("same-partition-second", "endpoint-a");
        WebhookEvent parallelWebhookEvent = event("different-partition", "endpoint-b");
        TEST_RECEIVER.blockResponses();
        try {
            this.sendToPartition(firstWebhookEvent, 0);
            this.sendToPartition(secondWebhookEvent, 0);
            this.sendToPartition(parallelWebhookEvent, 1);

            TEST_RECEIVER.awaitRequests(firstWebhookEvent.getEventId(), parallelWebhookEvent.getEventId());
            Assertions.assertThat(TEST_RECEIVER.count(secondWebhookEvent.getEventId())).isZero();
        } finally {
            TEST_RECEIVER.releaseResponses();
        }

        for (WebhookEvent webhookEvent : List.of(firstWebhookEvent, secondWebhookEvent, parallelWebhookEvent)) {
            this.awaitAttemptRows(webhookEvent.getEventId(), 1);
        }
    }

    private void sendToPartition(WebhookEvent webhookEvent, int partition) {
        this.producerTemplate.sendBodyAndHeaders("kafka:" + WebhookRoute.WEBHOOK_EVENT_TOPIC, webhookEvent, Map.of(KafkaConstants.KEY, webhookEvent.getEndpointId(), KafkaConstants.PARTITION_KEY, partition));
    }

    @Test
    void duplicateSourceDeliveryPostsAgainWithoutDuplicatingThePersistedAttemptNumber() throws Exception {
        String eventId = UUID.randomUUID().toString();
        WebhookEvent webhookEvent = new WebhookEvent(eventId, "duplicate.test", TENANT_ID, ENDPOINT_ID, Instant.now(), ByteBuffer.wrap("{\"duplicate\":true}".getBytes(StandardCharsets.UTF_8)), 0, null, 0, 0);

        try (
                AdminClient adminClient = KafkaTestRecords.adminClient();
                KafkaConsumer<String, Object> attemptKafkaConsumer = this.observeNewRecords(adminClient, WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC)
        ) {
            Map<String, Object> headers = Map.of(KafkaConstants.KEY, webhookEvent.getEndpointId());
            this.producerTemplate.sendBodyAndHeaders("kafka:" + WebhookRoute.WEBHOOK_EVENT_TOPIC, webhookEvent, headers);
            KafkaTestRecords.awaitCommit(adminClient, KafkaTestRecords.awaitRecord(attemptKafkaConsumer, eventId));

            this.producerTemplate.sendBodyAndHeaders("kafka:" + WebhookRoute.WEBHOOK_EVENT_TOPIC, webhookEvent, headers);
            KafkaTestRecords.awaitCommit(adminClient, KafkaTestRecords.awaitRecord(attemptKafkaConsumer, eventId));

            Assertions.assertThat(TEST_RECEIVER.count(eventId)).isEqualTo(2);
            Assertions.assertThat(this.attemptRowCount(eventId)).isEqualTo(1);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {
            false,
            true
    })
    void persistsPostgresqlJsonbAndTreatsDuplicateAttemptEnvelopesAsSuccess(boolean deadLettered) {
        String attemptId = UUID.randomUUID().toString();
        String eventId = UUID.randomUUID().toString();
        int retryCount = deadLettered ? 3 : 2;
        DeliveryAttempt originalDeliveryAttempt = new DeliveryAttempt(
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
                retryCount,
                1,
                deadLettered
        );
        DeliveryAttempt duplicateDeliveryAttempt = new DeliveryAttempt(
                UUID.randomUUID().toString(),
                eventId,
                TENANT_ID,
                ENDPOINT_ID,
                "duplicate.test",
                3,
                deadLettered ? DeliveryOutcome.SUCCEEDED : DeliveryOutcome.PERMANENT_FAILURE,
                deadLettered ? 204 : 400,
                1L,
                Instant.parse("2026-07-10T12:01:00Z"),
                ByteBuffer.wrap((deadLettered ? "{}" : "{\"type\":\"HTTP_FAILURE\"}").getBytes(StandardCharsets.UTF_8)),
                2,
                1,
                !deadLettered
        );

        Assertions.assertThatCode(() -> {
            this.webhookService.persistDeliveryAttempt(originalDeliveryAttempt);
            this.webhookService.persistDeliveryAttempt(duplicateDeliveryAttempt);
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
        Map<String, Object> attemptColumns = this.jdbcTemplate.queryForMap(
                """
                SELECT attempt_id, event_id, tenant_id, endpoint_id, event_type, attempt_number, outcome,
                       http_status, latency_ms, extract(epoch from attempted_at)::bigint AS attempted_at_epoch,
                       retry_count, replay_count, dead_lettered, details::text AS details
                FROM delivery_attempt
                WHERE event_id = ?::uuid
                """,
                eventId
        );
        Assertions.assertThat(attemptColumns.get("attempt_id").toString()).isEqualTo(attemptId);
        Assertions.assertThat(attemptColumns.get("event_id").toString()).isEqualTo(eventId);
        Assertions.assertThat(attemptColumns.get("tenant_id").toString()).isEqualTo(TENANT_ID);
        Assertions.assertThat(attemptColumns.get("endpoint_id").toString()).isEqualTo(ENDPOINT_ID);
        Assertions.assertThat(attemptColumns)
                .containsEntry("event_type", "persistence.test")
                .containsEntry("attempted_at_epoch", Instant.parse("2026-07-10T12:00:00Z").getEpochSecond())
                .containsEntry("attempt_number", 3)
                .containsEntry("outcome", "RETRYABLE_FAILURE")
                .containsEntry("latency_ms", 42L);
        Assertions.assertThat(attemptColumns.get("http_status")).isNull();
        Assertions.assertThat(attemptColumns).containsEntry("retry_count", retryCount).containsEntry("replay_count", 1).containsEntry("dead_lettered", deadLettered);
        Assertions.assertThat(attemptColumns.get("details").toString()).contains("\"type\": \"TIMEOUT\"");

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
                .extracting(columnMetadata -> List.of(columnMetadata.get("column_name"), columnMetadata.get("data_type"), columnMetadata.get("is_nullable")))
                .containsExactly(List.of("dead_lettered", "boolean", "NO"), List.of("replay_count", "integer", "NO"), List.of("retry_count", "integer", "NO"));
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
        List<String> topics = DeadLetterTestRecords.APPLICATION_TOPICS;
        try (AdminClient adminClient = KafkaTestRecords.adminClient()) {
            Map<String, TopicDescription> topicDescriptions = adminClient.describeTopics(topics).allTopicNames().get(10, TimeUnit.SECONDS);
            Assertions.assertThat(topicDescriptions.keySet()).containsExactlyInAnyOrderElementsOf(topics);
            Assertions.assertThat(topicDescriptions.values()).allSatisfy(topicDescription -> {
                Assertions.assertThat(topicDescription.partitions()).hasSize(12);
                Assertions.assertThat(topicDescription.partitions()).allSatisfy(topicPartitionInfo -> Assertions.assertThat(topicPartitionInfo.replicas()).hasSize(1));
            });

            Map<String, String> expectedRetentions = Map.of(
                    "webhook-events", "604800000",
                    "persist-delivery-attempt", "2592000000",
                    "webhook-events-retry-1m", "604800000",
                    "webhook-events-retry-10m", "604800000",
                    "webhook-events-retry-1h", "604800000",
                    "webhook-events-dead-letter", "2592000000"
            );
            List<ConfigResource> configResources = topics.stream().map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic)).toList();
            Map<ConfigResource, Config> configs = adminClient.describeConfigs(configResources).all().get(10, TimeUnit.SECONDS);
            for (ConfigResource configResource : configResources) {
                Assertions.assertThat(configValue(configs.get(configResource), TopicConfig.CLEANUP_POLICY_CONFIG)).isEqualTo(TopicConfig.CLEANUP_POLICY_DELETE);
                Assertions.assertThat(configValue(configs.get(configResource), TopicConfig.RETENTION_MS_CONFIG)).isEqualTo(expectedRetentions.get(configResource.name()));
            }
            assertActiveTopicNamedGroups(adminClient);
        }
        List<KafkaEndpoint> kafkaEndpoints = this.camelContext.getRoutes().stream().map(Route::getEndpoint).filter(KafkaEndpoint.class::isInstance).map(KafkaEndpoint.class::cast).toList();
        Assertions.assertThat(kafkaEndpoints).extracting(kafkaEndpoint -> kafkaEndpoint.getConfiguration().getTopic()).containsExactlyInAnyOrderElementsOf(topics);
        for (String topic : topics) {
            String routeId = KafkaTestRecords.consumerRoute(topic);
            Assertions.assertThat(this.camelContext.getRouteController().getRouteStatus(routeId).isStarted()).as(routeId).isTrue();
            KafkaEndpoint kafkaEndpoint = (KafkaEndpoint) this.camelContext.getRoute(routeId).getEndpoint();
            Assertions.assertThat(kafkaEndpoint.getConfiguration().getGroupId()).isEqualTo(topic);
            boolean deadLetterTopic = Objects.equals(topic, DeadLetterTestRecords.GROUP_ID);
            Class<?> deserializerType = deadLetterTopic ? ByteArrayDeserializer.class : AvroKafkaDeserializer.class;
            Assertions.assertThat(kafkaEndpoint.getConfiguration().getValueDeserializer()).isEqualTo(deserializerType.getName());
            Assertions.assertThat(kafkaEndpoint.getConfiguration().getAdditionalProperties()).doesNotContainKey(KafkaSerdeConfig.ENABLE_HEADERS);
            if (deadLetterTopic) {
                Assertions.assertThat(kafkaEndpoint.getKafkaClientFactory()).isSameAs(this.deadLetterKafkaClientFactory);
            } else {
                Assertions.assertThat(kafkaEndpoint.getKafkaClientFactory()).isExactlyInstanceOf(DefaultKafkaClientFactory.class);
            }
        }
    }

    private static void assertActiveTopicNamedGroups(AdminClient adminClient) throws InterruptedException {
        List<String> topics = DeadLetterTestRecords.APPLICATION_TOPICS;
        ListGroupsOptions listGroupsOptions = ListGroupsOptions.forConsumerGroups().inGroupStates(Set.of(GroupState.STABLE, GroupState.PREPARING_REBALANCE, GroupState.COMPLETING_REBALANCE));
        Instant deadline = Instant.now().plusSeconds(25);
        while (Instant.now().isBefore(deadline)) {
            try {
                List<String> groupIds = adminClient.listGroups(listGroupsOptions).all().get(2, TimeUnit.SECONDS).stream().map(GroupListing::groupId).toList();
                List<ConsumerGroupDescription> applicationGroups = adminClient
                        .describeConsumerGroups(groupIds)
                        .all()
                        .get(2, TimeUnit.SECONDS)
                        .values()
                        .stream()
                        .filter(consumerGroupDescription -> consumerGroupDescription.members().stream().anyMatch(CoreWebhookDeliveryLoopIntegrationTests::isApplicationMember))
                        .toList();
                List<String> stableGroupIds = applicationGroups.stream().filter(consumerGroupDescription -> Objects.equals(consumerGroupDescription.groupState(), GroupState.STABLE)).map(ConsumerGroupDescription::groupId).toList();
                if (stableGroupIds.containsAll(topics)) {
                    Assertions.assertThat(applicationGroups).extracting(ConsumerGroupDescription::groupId).containsExactlyInAnyOrderElementsOf(topics);
                    for (ConsumerGroupDescription consumerGroupDescription : applicationGroups) {
                        List<String> assignedTopics = consumerGroupDescription
                                .members()
                                .stream()
                                .filter(CoreWebhookDeliveryLoopIntegrationTests::isApplicationMember)
                                .flatMap(memberDescription -> memberDescription.assignment().topicPartitions().stream())
                                .map(TopicPartition::topic)
                                .toList();
                        Assertions.assertThat(assignedTopics).hasSize(12).containsOnly(consumerGroupDescription.groupId());
                    }
                    return;
                }
            } catch (ExecutionException | TimeoutException exception) {
                // The groups can be absent or rebalancing at startup.
            }
            Thread.sleep(50L);
        }
        throw new AssertionError("Application consumer groups did not become stable: " + topics);
    }

    private static boolean isApplicationMember(MemberDescription memberDescription) {
        return Objects.equals(memberDescription.clientId(), KAFKA_CLIENT_ID);
    }

    @Test
    void servesTheMockWebhookReceiverWithTheConfiguredSecret() throws Exception {
        byte[] bodyBytes = "{\"receiver\":\"é\"}".getBytes(StandardCharsets.UTF_8);
        Mockito.when(this.clock.instant()).thenReturn(Instant.ofEpochSecond(1_700_000_000L));
        String validSignature = "v1,32ULFHFh1wXI0KkMic9wq0iyvl0zQPZ1c8+PN6ODKQg=";

        Assertions.assertThat(this.postReceiver(bodyBytes, "receiver-event", "1700000000", validSignature).statusCode()).isEqualTo(204);
    }

    private HttpResponse<String> postEvent(String body) throws IOException, InterruptedException {
        HttpRequest httpRequest = HttpRequest
                .newBuilder()
                .uri(URI.create("http://localhost:" + this.serverPort + "/api/events"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
        return HttpClient.newHttpClient().send(httpRequest, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> postReceiver(byte[] bodyBytes, String webhookId, String timestamp, String signature) throws IOException, InterruptedException {
        HttpRequest httpRequest = HttpRequest
                .newBuilder()
                .uri(URI.create("http://localhost:" + this.serverPort + "/api/mocked-webhook-receiver"))
                .header("Content-Type", "application/json")
                .header("webhook-id", webhookId)
                .header("webhook-timestamp", timestamp)
                .header("webhook-signature", signature)
                .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes))
                .build();
        return HttpClient.newHttpClient().send(httpRequest, HttpResponse.BodyHandlers.ofString());
    }

    private KafkaConsumer<String, Object> observeNewRecords(AdminClient adminClient, String topic) throws Exception {
        return KafkaTestRecords.consumer(this.registryVertx, KafkaTestRecords.endOffsets(adminClient, topic));
    }

    @SuppressWarnings("SameParameterValue")
    private void awaitPartitionsAssignedToDifferentConsumers(String groupId, String topic, int firstPartition, int secondPartition) throws InterruptedException {
        TopicPartition firstTopicPartition = new TopicPartition(topic, firstPartition);
        TopicPartition secondTopicPartition = new TopicPartition(topic, secondPartition);
        Instant deadline = Instant.now().plusSeconds(15);
        try (AdminClient adminClient = KafkaTestRecords.adminClient()) {
            while (Instant.now().isBefore(deadline)) {
                try {
                    ConsumerGroupDescription consumerGroupDescription = adminClient.describeConsumerGroups(List.of(groupId)).all().get(2, TimeUnit.SECONDS).get(groupId);
                    MemberDescription firstMemberDescription = ownerOf(consumerGroupDescription, firstTopicPartition);
                    MemberDescription secondMemberDescription = ownerOf(consumerGroupDescription, secondTopicPartition);
                    if (Objects.equals(consumerGroupDescription.groupState(), GroupState.STABLE)
                            && consumerGroupDescription.members().size() == 12
                            && Objects.nonNull(firstMemberDescription)
                            && Objects.nonNull(secondMemberDescription)
                            && !Objects.equals(firstMemberDescription.consumerId(), secondMemberDescription.consumerId())) {
                        return;
                    }
                } catch (ExecutionException | TimeoutException exception) {
                    // The group can be absent or rebalancing while the application consumers start.
                }
                Thread.sleep(50L);
            }
        }
        throw new AssertionError("Partitions " + firstPartition + " and " + secondPartition + " were not assigned to different consumers in stable group " + groupId);
    }

    private static MemberDescription ownerOf(ConsumerGroupDescription consumerGroupDescription, TopicPartition topicPartition) {
        return consumerGroupDescription.members().stream().filter(memberDescription -> memberDescription.assignment().topicPartitions().contains(topicPartition)).findFirst().orElse(null);
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
        Integer count = this.jdbcTemplate.queryForObject("SELECT count(*) FROM delivery_attempt WHERE event_id = ?::uuid", Integer.class, eventId);
        return Objects.isNull(count) ? 0 : count;
    }

    private static void assertPhaseOneAuditFields(Map<String, Object> attemptColumns) {
        Assertions.assertThat(attemptColumns).containsEntry("retry_count", 0).containsEntry("replay_count", 0).containsEntry("dead_lettered", false);
    }

    private static byte[] bytes(ByteBuffer byteBuffer) {
        ByteBuffer duplicateByteBuffer = byteBuffer.duplicate();
        byte[] bytes = new byte[duplicateByteBuffer.remaining()];
        duplicateByteBuffer.get(bytes);
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
        ConfigEntry configEntry = config.get(key);
        return Objects.isNull(configEntry) ? null : configEntry.value();
    }

    private record TerminalInput(int deliveryAttemptCount, int retryCount, Instant nextEligibleAt) {
    }

    private record CapturedRequest(byte[] bodyBytes, String eventType, String webhookId, String timestamp, String signature, String contentType) {
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static final class TestReceiver implements AutoCloseable {

        private final HttpServer httpServer;
        private final ExecutorService executorService;
        private final ConcurrentLinkedQueue<CapturedRequest> capturedRequests = new ConcurrentLinkedQueue<>();
        private final AtomicInteger responseStatusCounter = new AtomicInteger(204);
        private volatile String retryableEventId;
        private volatile TransportFailure transportFailure;
        private volatile byte[] responseBodyBytes = new byte[0];
        private volatile CountDownLatch responseCountDownLatch;

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
                throw new IllegalStateException("Could not start integration receiver", exception);
            }
        }

        String uri() {
            return "http://localhost:" + this.httpServer.getAddress().getPort() + "/webhook";
        }

        void respondWith(int responseStatus) {
            this.responseStatusCounter.set(responseStatus);
        }

        void respondWith(int responseStatus, String body) {
            this.responseStatusCounter.set(responseStatus);
            this.responseBodyBytes = body.getBytes(StandardCharsets.UTF_8);
        }

        void failOnly(String eventId) {
            this.retryableEventId = eventId;
        }

        void clearRetryableFailure() {
            this.retryableEventId = null;
        }

        void failTransportOnly(String eventId, boolean timeout) {
            this.transportFailure = new TransportFailure(eventId, timeout, new CountDownLatch(1));
        }

        void clearTransportFailure() {
            TransportFailure transportFailure = this.transportFailure;
            if (Objects.nonNull(transportFailure)) {
                transportFailure.responseCountDownLatch().countDown();
            }
            this.transportFailure = null;
        }

        void blockResponses() {
            this.responseCountDownLatch = new CountDownLatch(1);
        }

        void releaseResponses() {
            CountDownLatch responseCountDownLatch = this.responseCountDownLatch;
            if (Objects.nonNull(responseCountDownLatch)) {
                responseCountDownLatch.countDown();
            }
        }

        void reset() {
            this.releaseResponses();
            this.capturedRequests.clear();
            this.responseStatusCounter.set(204);
            this.clearRetryableFailure();
            this.clearTransportFailure();
            this.responseBodyBytes = new byte[0];
            this.responseCountDownLatch = null;
        }

        int count(String eventId) {
            return (int) this.capturedRequests.stream().filter(capturedRequest -> Objects.equals(eventId, capturedRequest.webhookId())).count();
        }

        CapturedRequest awaitRequest(String eventId, int expectedCount) throws InterruptedException {
            Instant deadline = Instant.now().plusSeconds(30);
            while (Instant.now().isBefore(deadline)) {
                List<CapturedRequest> matchingCapturedRequests = this.capturedRequests.stream().filter(capturedRequest -> Objects.equals(eventId, capturedRequest.webhookId())).toList();
                if (matchingCapturedRequests.size() >= expectedCount) {
                    return matchingCapturedRequests.get(expectedCount - 1);
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

        private void handle(HttpExchange httpExchange) throws IOException {
            byte[] bodyBytes = httpExchange.getRequestBody().readAllBytes();
            String eventId = httpExchange.getRequestHeaders().getFirst("webhook-id");
            this.capturedRequests.add(new CapturedRequest(
                    bodyBytes,
                    httpExchange.getRequestHeaders().getFirst("webhook-event-type"),
                    eventId,
                    httpExchange.getRequestHeaders().getFirst("webhook-timestamp"),
                    httpExchange.getRequestHeaders().getFirst("webhook-signature"),
                    httpExchange.getRequestHeaders().getFirst("Content-Type")
            ));
            try (httpExchange) {
                TransportFailure transportFailure = this.transportFailure;
                if (Objects.nonNull(transportFailure) && Objects.equals(transportFailure.eventId(), eventId)) {
                    if (transportFailure.timeout()) {
                        transportFailure.responseCountDownLatch().await(30, TimeUnit.SECONDS);
                    }
                    return;
                }
                CountDownLatch responseCountDownLatch = this.responseCountDownLatch;
                if (Objects.nonNull(responseCountDownLatch)) {
                    responseCountDownLatch.await(10, TimeUnit.SECONDS);
                }
                int responseStatus = Objects.nonNull(eventId) && Objects.equals(eventId, this.retryableEventId) ? 503 : this.responseStatusCounter.get();
                if (responseStatus == 302) {
                    httpExchange.getResponseHeaders().add("Location", "/redirect-target");
                }
                byte[] responseBytes = this.responseBodyBytes;
                httpExchange.sendResponseHeaders(responseStatus, responseBytes.length == 0 ? -1L : responseBytes.length);
                if (responseBytes.length > 0) {
                    httpExchange.getResponseBody().write(responseBytes);
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void close() {
            this.releaseResponses();
            this.httpServer.stop(0);
            this.executorService.shutdownNow();
        }

        private record TransportFailure(String eventId, boolean timeout, CountDownLatch responseCountDownLatch) {
        }
    }
}
