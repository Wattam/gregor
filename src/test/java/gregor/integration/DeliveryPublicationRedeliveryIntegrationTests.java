package gregor.integration;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import gregor.avro.DeliveryAttempt;
import gregor.avro.DeliveryOutcome;
import gregor.avro.WebhookEvent;
import gregor.route.WebhookRoute;
import io.vertx.core.Vertx;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.component.kafka.DefaultKafkaClientFactory;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.spi.RouteController;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.apache.camel.test.spring.junit5.UseAdviceWith;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

@UseAdviceWith
@CamelSpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = "gregor.webhook.maximum-attempts=4")
class DeliveryPublicationRedeliveryIntegrationTests {

    private static final Instant NOW = Instant.parse("2026-07-25T12:00:00.123456789Z");
    private static final String ATTEMPT_TOPIC = WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC;
    private static final String[] DELAYED_TOPICS = {WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC, WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC, WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC, WebhookRoute.WEBHOOK_EVENT_DEAD_LETTER_TOPIC};
    private static final String SIGNING_SECRET = "whsec_MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE=";
    private static final TestReceiver TEST_RECEIVER = TestReceiver.start();

    @Autowired
    private CamelContext camelContext;
    @Autowired
    private ProducerTemplate producerTemplate;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private Vertx registryVertx;
    @MockitoBean
    private Clock clock;

    @DynamicPropertySource
    static void configureInfrastructure(DynamicPropertyRegistry dynamicPropertyRegistry) {
        String postgresHost = System.getenv().getOrDefault("POSTGRES_HOST", "localhost");
        String postgresPort = System.getenv().getOrDefault("POSTGRES_PORT", "5432");
        String postgresDatabase = System.getenv().getOrDefault("POSTGRES_DB", "gregor");
        String kafkaPort = System.getenv().getOrDefault("KAFKA_PORT", "9092");

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
        dynamicPropertyRegistry.add("camel.component.kafka.shutdown-timeout", () -> "1000");
        dynamicPropertyRegistry.add("camel.component.kafka.request-required-acks", () -> "all");
        dynamicPropertyRegistry.add("gregor.webhook.receiver-url", TEST_RECEIVER::uri);
        dynamicPropertyRegistry.add("gregor.webhook.signing-secret", () -> SIGNING_SECRET);
    }

    @AfterAll
    static void stopReceiver() {
        TEST_RECEIVER.close();
    }

    @ParameterizedTest
    @CsvSource({
            "webhook-events, ATTEMPT, webhook-events-retry-1m, 2026-07-25T12:01:00.123Z",
            "webhook-events, RETRY, webhook-events-retry-1m, 2026-07-25T12:01:00.123Z",
            "webhook-events-retry-1m, ATTEMPT, webhook-events-retry-1h, 2026-07-25T13:00:00.123Z",
            "webhook-events-retry-1m, RETRY, webhook-events-retry-1h, 2026-07-25T13:00:00.123Z"
    })
    void redeliversTheSameLogicalAttemptAndCommitsOnlyAfterBothPublications(String inputTopic, FailedPublication failedPublication, String retryTopic, Instant due) throws Exception {
        AtomicReference<Instant> currentInstantReference = new AtomicReference<>(NOW);
        Mockito.when(this.clock.instant()).thenAnswer(_ -> currentInstantReference.get());
        boolean fromSourceTopic = inputTopic.equals(WebhookRoute.WEBHOOK_EVENT_TOPIC);
        int auditNumber = fromSourceTopic ? 1 : 2;
        int auditRetry = fromSourceTopic ? 0 : 2;
        int replay = fromSourceTopic ? 0 : 3;
        int successorRetry = fromSourceTopic ? 1 : 3;
        String eventId = UUID.randomUUID().toString();
        byte[] payloadBytes = "{ \"private\": \"é\" }".getBytes(StandardCharsets.UTF_8);
        WebhookEvent webhookEvent = new WebhookEvent(
                eventId,
                "publication-failure.test",
                "00000000-0000-0000-0000-000000000001",
                "00000000-0000-0000-0000-000000000002",
                Instant.parse("2026-07-25T11:00:00Z"),
                ByteBuffer.wrap(payloadBytes),
                auditRetry,
                Instant.parse("2026-07-25T11:55:00Z"),
                fromSourceTopic ? 0 : 1,
                replay
        );
        PublicationAcknowledgements publicationAcknowledgements = new PublicationAcknowledgements(eventId, auditNumber, failedPublication);
        try (AdminClient adminClient = KafkaTestRecords.adminClient()) {
            this.camelContext.getComponent("kafka", KafkaComponent.class).setKafkaClientFactory(publicationAcknowledgements);
            this.observeDeliveryCommits(inputTopic, eventId, publicationAcknowledgements);
            this.startRoutes(inputTopic);
            RouteController routeController = this.camelContext.getRouteController();

            Map<TopicPartition, Long> inputOffsets = KafkaTestRecords.endOffsets(adminClient, inputTopic);
            TopicPartition inputTopicPartition = new TopicPartition(inputTopic, 0);
            long inputOffset = inputOffsets.get(inputTopicPartition);
            Map<TopicPartition, Long> attemptOffsets = KafkaTestRecords.endOffsets(adminClient, ATTEMPT_TOPIC);
            Map<TopicPartition, Long> delayedOffsets = KafkaTestRecords.endOffsets(adminClient, DELAYED_TOPICS);
            Map<TopicPartition, Long> retryOffsets = new HashMap<>(delayedOffsets);
            Map<TopicPartition, Long> successorOffsets = KafkaTestRecords.endOffsets(adminClient, retryTopic);
            try (
                    AutoCloseable _ = () -> {
                        TEST_RECEIVER.succeed(eventId);
                        publicationAcknowledgements.releaseAll();
                        try {
                            // A misrouted successor can share the input topic.
                            if (KafkaTestRecords.endOffsets(adminClient, inputTopic).get(inputTopicPartition) > inputOffset) {
                                KafkaTestRecords.awaitCommit(adminClient, inputTopicPartition, inputOffset);
                            }
                        } finally {
                            KafkaTestRecords.drainRetries(adminClient, routeController, delayedOffsets, currentInstantReference);
                        }
                    };
                    KafkaConsumer<String, Object> inputKafkaConsumer = KafkaTestRecords.consumer(this.registryVertx, inputOffsets);
                    KafkaConsumer<String, Object> attemptKafkaConsumer = KafkaTestRecords.consumer(this.registryVertx, attemptOffsets);
                    KafkaConsumer<String, Object> retryKafkaConsumer = KafkaTestRecords.consumer(this.registryVertx, successorOffsets)
            ) {
                TEST_RECEIVER.fail(eventId);
                this.producerTemplate.sendBodyAndHeaders("kafka:" + inputTopic, webhookEvent, Map.of(KafkaConstants.KEY, "wrong-inbound-key", KafkaConstants.PARTITION_KEY, inputTopicPartition.partition()));
                ConsumerRecord<String, Object> inputConsumerRecord = KafkaTestRecords.awaitRecord(inputKafkaConsumer, eventId);
                if (!fromSourceTopic) {
                    countRecord(retryOffsets, inputConsumerRecord);
                }
                PendingAcknowledgement firstAttemptAcknowledgement = publicationAcknowledgements.awaitAttempt();
                this.assertSignedRequests(eventId, payloadBytes, 1);
                assertUncommitted(adminClient, inputConsumerRecord);
                Assertions.assertThat(KafkaTestRecords.endOffsets(adminClient, DELAYED_TOPICS)).isEqualTo(retryOffsets);
                if (failedPublication == FailedPublication.ATTEMPT) {
                    firstAttemptAcknowledgement.assertFailed();
                    Assertions.assertThat(KafkaTestRecords.endOffsets(adminClient, ATTEMPT_TOPIC)).isEqualTo(attemptOffsets);
                } else {
                    firstAttemptAcknowledgement.assertSucceeded();
                    assertAudit(KafkaTestRecords.awaitRecord(attemptKafkaConsumer, eventId), eventId, auditNumber, auditRetry, replay);
                }
                firstAttemptAcknowledgement.release();

                if (failedPublication == FailedPublication.RETRY) {
                    PendingAcknowledgement failedRetryAcknowledgement = publicationAcknowledgements.awaitRetry();
                    failedRetryAcknowledgement.assertFailed();
                    assertUncommitted(adminClient, inputConsumerRecord);
                    Assertions.assertThat(KafkaTestRecords.endOffsets(adminClient, DELAYED_TOPICS)).isEqualTo(retryOffsets);
                    failedRetryAcknowledgement.release();
                }

                PendingAcknowledgement recoveredAttemptAcknowledgement = publicationAcknowledgements.awaitAttempt();
                recoveredAttemptAcknowledgement.assertSucceeded();
                ConsumerRecord<String, Object> recoveredAttemptConsumerRecord = KafkaTestRecords.awaitRecord(attemptKafkaConsumer, eventId);
                assertAudit(recoveredAttemptConsumerRecord, eventId, auditNumber, auditRetry, replay);
                this.assertSignedRequests(eventId, payloadBytes, 2);
                assertUncommitted(adminClient, inputConsumerRecord);
                Assertions.assertThat(KafkaTestRecords.endOffsets(adminClient, DELAYED_TOPICS)).isEqualTo(retryOffsets);
                recoveredAttemptAcknowledgement.release();

                PendingAcknowledgement recoveredRetryAcknowledgement = publicationAcknowledgements.awaitRetry();
                recoveredRetryAcknowledgement.assertSucceededOn(retryTopic);
                ConsumerRecord<String, Object> successorConsumerRecord = KafkaTestRecords.awaitRecord(retryKafkaConsumer, eventId);
                Assertions.assertThat(inputConsumerRecord.key()).isEqualTo("wrong-inbound-key");
                Assertions.assertThat(successorConsumerRecord.key()).isEqualTo(webhookEvent.getEndpointId());
                Assertions.assertThat(successorConsumerRecord.value()).isEqualTo(WebhookEvent.newBuilder(webhookEvent).setDeliveryAttemptCount(auditNumber).setRetryCount(successorRetry).setNextEligibleAt(due).build());
                assertUncommitted(adminClient, inputConsumerRecord);
                recoveredRetryAcknowledgement.release();

                KafkaTestRecords.awaitCommit(adminClient, inputConsumerRecord);
                publicationAcknowledgements.assertPublicationOrder();
                countRecord(retryOffsets, successorConsumerRecord);
                Assertions.assertThat(KafkaTestRecords.endOffsets(adminClient, DELAYED_TOPICS)).isEqualTo(retryOffsets);
                this.assertSignedRequests(eventId, payloadBytes, 2);
                Assertions.assertThat(this.attemptRows(eventId)).isEmpty();

                routeController.startRoute(KafkaTestRecords.consumerRoute(ATTEMPT_TOPIC));
                KafkaTestRecords.awaitCommit(adminClient, recoveredAttemptConsumerRecord);
                Assertions.assertThat(this.attemptRows(eventId)).containsExactly(Map.of("attempt_number", auditNumber, "retry_count", auditRetry, "replay_count", replay, "outcome", "RETRYABLE_FAILURE"));
            }
            this.assertSignedRequests(eventId, payloadBytes, 3);
        }
    }

    private void observeDeliveryCommits(String topic, String eventId, PublicationAcknowledgements publicationAcknowledgements) throws Exception {
        AtomicInteger deliveryCounter = new AtomicInteger();
        AdviceWith.adviceWith(
                this.camelContext,
                KafkaTestRecords.consumerRoute(topic),
                adviceWithRouteBuilder -> adviceWithRouteBuilder.weaveAddFirst().process(exchange -> {
                    if (eventId.equals(exchange.getMessage().getBody(WebhookEvent.class).getEventId())) {
                        int delivery = deliveryCounter.incrementAndGet();
                        KafkaManualCommit originalKafkaManualCommit = exchange.getMessage().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
                        KafkaManualCommit observedKafkaManualCommit = () -> {
                            publicationAcknowledgements.observeCommit(delivery);
                            originalKafkaManualCommit.commit();
                        };
                        exchange.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, observedKafkaManualCommit);
                    }
                })
        );
    }

    private static void countRecord(Map<TopicPartition, Long> offsets, ConsumerRecord<String, Object> consumerRecord) {
        offsets.compute(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()), (_, offset) -> offset + 1);
    }

    private void startRoutes(String inputTopic) throws Exception {
        this.camelContext.start();
        RouteController routeController = this.camelContext.getRouteController();
        for (String routeId : List.of(WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT, WebhookRoute.WEBHOOK_DELIVERY_ATTEMPT_DIRECT, WebhookRoute.WEBHOOK_RETRY_ELIGIBILITY_DIRECT, KafkaTestRecords.consumerRoute(inputTopic))) {
            routeController.startRoute(routeId);
        }
    }

    private static void assertUncommitted(AdminClient adminClient, ConsumerRecord<String, Object> consumerRecord) throws Exception {
        Assertions.assertThat(KafkaTestRecords.committedOffset(adminClient, consumerRecord)).isLessThan(consumerRecord.offset() + 1);
    }

    private static void assertAudit(ConsumerRecord<String, Object> consumerRecord, String eventId, int number, int retries, int replay) {
        Assertions.assertThat(consumerRecord.key()).isEqualTo(eventId);
        DeliveryAttempt deliveryAttempt = (DeliveryAttempt) consumerRecord.value();
        Assertions.assertThat(deliveryAttempt.getAttemptNumber()).isEqualTo(number);
        Assertions.assertThat(deliveryAttempt.getOutcome()).isEqualTo(DeliveryOutcome.RETRYABLE_FAILURE);
        Assertions.assertThat(deliveryAttempt.getHttpStatus()).isEqualTo(503);
        Assertions.assertThat(deliveryAttempt.getRetryCount()).isEqualTo(retries);
        Assertions.assertThat(deliveryAttempt.getReplayCount()).isEqualTo(replay);
        Assertions.assertThat(deliveryAttempt.getDeadLettered()).isFalse();
        Assertions.assertThat(deliveryAttempt.getSchema().getField("payload")).isNull();
        Assertions.assertThat(deliveryAttempt.getSchema().getField("signingSecret")).isNull();
        Assertions.assertThat(StandardCharsets.UTF_8.decode(deliveryAttempt.getDetails().duplicate()).toString()).doesNotContain("private", SIGNING_SECRET);
    }

    private List<Map<String, Object>> attemptRows(String eventId) {
        return this.jdbcTemplate.queryForList(
                """
                SELECT attempt_number, retry_count, replay_count, outcome
                FROM delivery_attempt WHERE event_id = ?::uuid
                """,
                eventId
        );
    }

    private void assertSignedRequests(String eventId, byte[] payloadBytes, int count) {
        Assertions.assertThat(TEST_RECEIVER.requests(eventId)).hasSize(count).allSatisfy(capturedRequest -> {
            Assertions.assertThat(capturedRequest.webhookId()).isEqualTo(eventId);
            Assertions.assertThat(capturedRequest.bodyBytes()).isEqualTo(payloadBytes);
            Assertions.assertThat(capturedRequest.signature()).isEqualTo(WebhookSignatureFixtures.expectedSignature(SIGNING_SECRET, eventId, capturedRequest.timestamp(), payloadBytes));
        });
    }

    private enum FailedPublication {
        ATTEMPT, RETRY
    }

    private static final class PublicationAcknowledgements extends DefaultKafkaClientFactory {

        private final String eventId;
        private final int auditNumber;
        private final FailedPublication failedPublication;
        private final BlockingQueue<PendingAcknowledgement> attemptAcknowledgements = new LinkedBlockingQueue<>();
        private final BlockingQueue<PendingAcknowledgement> retryAcknowledgements = new LinkedBlockingQueue<>();
        private final CompletableFuture<Void> closedFuture = new CompletableFuture<>();
        private final AtomicInteger attemptSends = new AtomicInteger();
        private final AtomicInteger retrySends = new AtomicInteger();
        private final AtomicInteger attemptsAcknowledged = new AtomicInteger();
        private final AtomicBoolean retryAcknowledged = new AtomicBoolean();
        private final AtomicBoolean retriedBeforeAttemptAcknowledgement = new AtomicBoolean();
        private final AtomicBoolean committedBeforeRetryAcknowledgement = new AtomicBoolean();

        PublicationAcknowledgements(String eventId, int auditNumber, FailedPublication failedPublication) {
            this.eventId = eventId;
            this.auditNumber = auditNumber;
            this.failedPublication = failedPublication;
        }

        @Override
        public Producer<Object, Object> getProducer(Properties properties) {
            return new KafkaProducer<>(properties) {
                @Override
                public Future<RecordMetadata> send(ProducerRecord<Object, Object> producerRecord, Callback callback) {
                    return PublicationAcknowledgements.this.send(producerRecord, callback, super::send);
                }
            };
        }

        private Future<RecordMetadata> send(ProducerRecord<Object, Object> producerRecord, Callback callback, BiFunction<ProducerRecord<Object, Object>, Callback, Future<RecordMetadata>> sendFunction) {
            return switch (producerRecord.value()) {
                case DeliveryAttempt deliveryAttempt when this.eventId.equals(deliveryAttempt.getEventId()) && deliveryAttempt.getAttemptNumber() == this.auditNumber -> {
                    int send = this.attemptSends.incrementAndGet();
                    boolean fail = send == 1 && this.failedPublication == FailedPublication.ATTEMPT;
                    yield this.holdReply(producerRecord, callback, sendFunction, this.attemptAcknowledgements, this.attemptsAcknowledged::incrementAndGet, fail);
                }
                case WebhookEvent webhookEvent when this.eventId.equals(webhookEvent.getEventId()) && webhookEvent.getDeliveryAttemptCount() == this.auditNumber -> {
                    int send = this.retrySends.incrementAndGet();
                    if (this.attemptsAcknowledged.get() != send) {
                        this.retriedBeforeAttemptAcknowledgement.set(true);
                    }
                    boolean fail = send == 1 && this.failedPublication == FailedPublication.RETRY;
                    yield this.holdReply(producerRecord, callback, sendFunction, this.retryAcknowledgements, () -> this.retryAcknowledged.set(true), fail);
                }
                default -> sendFunction.apply(producerRecord, callback);
            };
        }

        private Future<RecordMetadata> holdReply(
                ProducerRecord<Object, Object> producerRecord,
                Callback callback,
                BiFunction<ProducerRecord<Object, Object>, Callback, Future<RecordMetadata>> sendFunction,
                BlockingQueue<PendingAcknowledgement> pendingAcknowledgements,
                Runnable onAcknowledged,
                boolean fail
        ) {
            PendingAcknowledgement pendingAcknowledgement = new PendingAcknowledgement(callback, onAcknowledged, this.closedFuture);
            if (fail) {
                pendingAcknowledgement.receive(null, new IllegalStateException("Injected send failure on " + producerRecord.topic()));
            } else {
                sendFunction.apply(producerRecord, pendingAcknowledgement::receive);
            }
            pendingAcknowledgements.add(pendingAcknowledgement);
            return pendingAcknowledgement.clientResultFuture;
        }

        PendingAcknowledgement awaitAttempt() throws Exception {
            return awaitReply(this.attemptAcknowledgements, "attempt");
        }

        PendingAcknowledgement awaitRetry() throws Exception {
            return awaitReply(this.retryAcknowledgements, "retry");
        }

        private static PendingAcknowledgement awaitReply(BlockingQueue<PendingAcknowledgement> pendingAcknowledgements, String name) throws Exception {
            PendingAcknowledgement pendingAcknowledgement = pendingAcknowledgements.poll(20, TimeUnit.SECONDS);
            if (pendingAcknowledgement == null) {
                throw new AssertionError("Kafka send did not start: " + name);
            }
            pendingAcknowledgement.brokerReplyFuture.get(20, TimeUnit.SECONDS);
            return pendingAcknowledgement;
        }

        void observeCommit(int delivery) {
            if (delivery != 2 || !this.retryAcknowledged.get()) {
                this.committedBeforeRetryAcknowledgement.set(true);
            }
        }

        void assertPublicationOrder() {
            Assertions.assertThat(this.retriedBeforeAttemptAcknowledgement).as("retry started before the attempt acknowledgement").isFalse();
            Assertions.assertThat(this.committedBeforeRetryAcknowledgement).as("only recovered delivery may commit, and only after the retry acknowledgement").isFalse();
        }

        void releaseAll() {
            this.closedFuture.complete(null);
        }
    }

    private static final class PendingAcknowledgement {

        private final CompletableFuture<BrokerReply> brokerReplyFuture = new CompletableFuture<>();
        private final CompletableFuture<Void> releaseFuture = new CompletableFuture<>();
        private final CompletableFuture<RecordMetadata> clientResultFuture = new CompletableFuture<>();

        PendingAcknowledgement(Callback callback, Runnable onAcknowledged, CompletableFuture<Void> closedFuture) {
            // Withhold both Kafka send completion APIs without blocking the producer's I/O thread.
            this.brokerReplyFuture.thenAcceptBoth(
                    this.releaseFuture.applyToEither(closedFuture, _ -> null),
                    (brokerReply, _) -> {
                        if (brokerReply.exception() == null) {
                            onAcknowledged.run();
                        }
                        if (callback != null) {
                            callback.onCompletion(brokerReply.recordMetadata(), brokerReply.exception());
                        }
                        if (brokerReply.exception() == null) {
                            this.clientResultFuture.complete(brokerReply.recordMetadata());
                        } else {
                            this.clientResultFuture.completeExceptionally(brokerReply.exception());
                        }
                    }
            );
        }

        void receive(RecordMetadata recordMetadata, Exception exception) {
            this.brokerReplyFuture.complete(new BrokerReply(recordMetadata, exception));
        }

        void assertSucceeded() {
            Assertions.assertThat(this.brokerReplyFuture.join().exception()).isNull();
        }

        void assertSucceededOn(String topic) {
            this.assertSucceeded();
            Assertions.assertThat(this.brokerReplyFuture.join().recordMetadata().topic()).isEqualTo(topic);
        }

        void assertFailed() {
            Assertions.assertThat(this.brokerReplyFuture.join().exception()).isInstanceOf(IllegalStateException.class);
        }

        void release() {
            this.releaseFuture.complete(null);
        }
    }

    private record BrokerReply(RecordMetadata recordMetadata, Exception exception) {
    }

    private record CapturedRequest(String webhookId, String timestamp, String signature, byte[] bodyBytes) {
    }

    private static final class TestReceiver implements AutoCloseable {

        private final HttpServer httpServer;
        private final ExecutorService executorService;
        private final Set<String> failingEventIds = ConcurrentHashMap.newKeySet();
        private final Map<String, ConcurrentLinkedQueue<CapturedRequest>> capturedRequestsByEventId = new ConcurrentHashMap<>();

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
                throw new IllegalStateException("Could not start redelivery receiver", exception);
            }
        }

        String uri() {
            return "http://localhost:" + this.httpServer.getAddress().getPort() + "/webhook";
        }

        void fail(String eventId) {
            this.failingEventIds.add(eventId);
        }

        void succeed(String eventId) {
            this.failingEventIds.remove(eventId);
        }

        List<CapturedRequest> requests(String eventId) {
            ConcurrentLinkedQueue<CapturedRequest> matchingCapturedRequests = this.capturedRequestsByEventId.get(eventId);
            return matchingCapturedRequests == null ? List.of() : new ArrayList<>(matchingCapturedRequests);
        }

        private void handle(HttpExchange httpExchange) throws IOException {
            try (httpExchange) {
                byte[] bodyBytes = httpExchange.getRequestBody().readAllBytes();
                String eventId = httpExchange.getRequestHeaders().getFirst("webhook-id");
                this.capturedRequestsByEventId
                        .computeIfAbsent(eventId, _ -> new ConcurrentLinkedQueue<>())
                        .add(new CapturedRequest(eventId, httpExchange.getRequestHeaders().getFirst("webhook-timestamp"), httpExchange.getRequestHeaders().getFirst("webhook-signature"), bodyBytes));
                if (!this.failingEventIds.contains(eventId)) {
                    httpExchange.sendResponseHeaders(204, -1L);
                } else {
                    byte[] echoedBytes = (new String(bodyBytes, StandardCharsets.UTF_8) + SIGNING_SECRET).getBytes(StandardCharsets.UTF_8);
                    httpExchange.sendResponseHeaders(503, echoedBytes.length);
                    httpExchange.getResponseBody().write(echoedBytes);
                }
            }
        }

        @Override
        public void close() {
            this.httpServer.stop(0);
            this.executorService.shutdownNow();
        }
    }
}
