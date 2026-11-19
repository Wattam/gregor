package gregor.integration;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import gregor.avro.DeliveryAttempt;
import gregor.avro.DeliveryOutcome;
import gregor.avro.WebhookEvent;
import gregor.component.SignatureComponent;
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
    private static final String[] DELAYED_TOPICS = {
            WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC, WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC,
            WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC, WebhookRoute.WEBHOOK_EVENT_DEAD_LETTER_TOPIC
    };
    private static final String SIGNING_SECRET = "whsec_MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE=";
    private static final TestReceiver RECEIVER = TestReceiver.start();

    @Autowired
    private CamelContext camelContext;
    @Autowired
    private ProducerTemplate producerTemplate;
    @Autowired
    private SignatureComponent signatureComponent;
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

        dynamicPropertyRegistry.add(
                "spring.datasource.url",
                () -> "jdbc:postgresql://" + postgresHost + ":" + postgresPort + "/" + postgresDatabase
        );
        dynamicPropertyRegistry.add(
                "spring.datasource.username",
                () -> System.getenv().getOrDefault("POSTGRES_USER", "gregor")
        );
        dynamicPropertyRegistry.add(
                "spring.datasource.password",
                () -> System.getenv().getOrDefault("POSTGRES_PASSWORD", "gregor")
        );
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
        dynamicPropertyRegistry.add("gregor.webhook.receiver-url", RECEIVER::uri);
        dynamicPropertyRegistry.add("gregor.webhook.signing-secret", () -> SIGNING_SECRET);
    }

    @AfterAll
    static void stopReceiver() {
        RECEIVER.close();
    }

    @ParameterizedTest
    @CsvSource({
            "webhook-events,          ATTEMPT, webhook-events-retry-1m, 2026-07-25T12:01:00.123Z",
            "webhook-events,          RETRY,   webhook-events-retry-1m, 2026-07-25T12:01:00.123Z",
            "webhook-events-retry-1m, ATTEMPT, webhook-events-retry-1h, 2026-07-25T13:00:00.123Z",
            "webhook-events-retry-1m, RETRY,   webhook-events-retry-1h, 2026-07-25T13:00:00.123Z"
    })
    void redeliversTheSameLogicalAttemptAndCommitsOnlyAfterBothPublications(
            String inputTopic,
            FailedPublication failedPublication,
            String retryTopic,
            Instant due
    ) throws Exception {
        AtomicReference<Instant> now = new AtomicReference<>(NOW);
        Mockito.when(this.clock.instant()).thenAnswer(_ -> now.get());
        boolean source = inputTopic.equals(WebhookRoute.WEBHOOK_EVENT_TOPIC);
        int auditNumber = source ? 1 : 2;
        int auditRetry = source ? 0 : 2;
        int replay = source ? 0 : 3;
        int successorRetry = source ? 1 : 3;
        String eventId = UUID.randomUUID().toString();
        byte[] payload = "{ \"private\": \"é\" }".getBytes(StandardCharsets.UTF_8);
        WebhookEvent webhookEvent = new WebhookEvent(
                eventId,
                "publication-failure.test",
                "00000000-0000-0000-0000-000000000001",
                "00000000-0000-0000-0000-000000000002",
                Instant.parse("2026-07-25T11:00:00Z"),
                ByteBuffer.wrap(payload),
                auditRetry,
                Instant.parse("2026-07-25T11:55:00Z"),
                source ? 0 : 1,
                replay
        );
        PublicationAcknowledgements publicationAcknowledgements =
                new PublicationAcknowledgements(eventId, auditNumber, failedPublication);
        try (AdminClient adminClient = KafkaTestRecords.adminClient()) {
            this.camelContext.getComponent("kafka", KafkaComponent.class)
                    .setKafkaClientFactory(publicationAcknowledgements);
            this.observeDeliveryCommits(inputTopic, eventId, publicationAcknowledgements);
            this.startRoutes(inputTopic);
            RouteController routeController = this.camelContext.getRouteController();

            Map<TopicPartition, Long> inputOffsets = KafkaTestRecords.endOffsets(adminClient, inputTopic);
            TopicPartition inputPartition = new TopicPartition(inputTopic, 0);
            long inputOffset = inputOffsets.get(inputPartition);
            Map<TopicPartition, Long> attemptOffsets = KafkaTestRecords.endOffsets(adminClient, ATTEMPT_TOPIC);
            Map<TopicPartition, Long> delayedOffsets = KafkaTestRecords.endOffsets(adminClient, DELAYED_TOPICS);
            Map<TopicPartition, Long> retryOffsets = new HashMap<>(delayedOffsets);
            Map<TopicPartition, Long> successorOffsets = KafkaTestRecords.endOffsets(adminClient, retryTopic);
            try (
                    AutoCloseable _ = () -> {
                        RECEIVER.succeed(eventId);
                        publicationAcknowledgements.releaseAll();
                        try {
                            // A misrouted successor can share the input topic.
                            if (KafkaTestRecords
                                    .endOffsets(adminClient, inputTopic)
                                    .get(inputPartition) > inputOffset) {
                                KafkaTestRecords.awaitCommit(adminClient, inputPartition, inputOffset);
                            }
                        } finally {
                            KafkaTestRecords.drainRetries(adminClient, routeController, delayedOffsets, now);
                        }
                    };
                    KafkaConsumer<String, Object> input = KafkaTestRecords.consumer(this.registryVertx, inputOffsets);
                    KafkaConsumer<String, Object> audit = KafkaTestRecords.consumer(this.registryVertx, attemptOffsets);
                    KafkaConsumer<String, Object> retries =
                            KafkaTestRecords.consumer(this.registryVertx, successorOffsets)
            ) {
                RECEIVER.fail(eventId);
                this.producerTemplate.sendBodyAndHeaders(
                        "kafka:" + inputTopic, webhookEvent,
                        Map.of(
                                KafkaConstants.KEY, "wrong-inbound-key",
                                KafkaConstants.PARTITION_KEY, inputPartition.partition()
                        )
                );
                ConsumerRecord<String, Object> original = KafkaTestRecords.awaitRecord(input, eventId);
                if (!source) {
                    countRecord(retryOffsets, original);
                }
                PendingAcknowledgement firstAttempt = publicationAcknowledgements.awaitAttempt();
                this.assertSignedRequests(eventId, payload, 1);
                assertUncommitted(adminClient, original);
                Assertions.assertThat(KafkaTestRecords.endOffsets(adminClient, DELAYED_TOPICS)).isEqualTo(retryOffsets);
                if (failedPublication == FailedPublication.ATTEMPT) {
                    firstAttempt.assertFailed();
                    Assertions.assertThat(KafkaTestRecords.endOffsets(adminClient, ATTEMPT_TOPIC))
                            .isEqualTo(attemptOffsets);
                } else {
                    firstAttempt.assertSucceeded();
                    assertAudit(KafkaTestRecords.awaitRecord(audit, eventId), eventId, auditNumber, auditRetry, replay);
                }
                firstAttempt.release();

                if (failedPublication == FailedPublication.RETRY) {
                    PendingAcknowledgement failedRetry = publicationAcknowledgements.awaitRetry();
                    failedRetry.assertFailed();
                    assertUncommitted(adminClient, original);
                    Assertions.assertThat(KafkaTestRecords.endOffsets(adminClient, DELAYED_TOPICS))
                            .isEqualTo(retryOffsets);
                    failedRetry.release();
                }

                PendingAcknowledgement recoveredAttempt = publicationAcknowledgements.awaitAttempt();
                recoveredAttempt.assertSucceeded();
                ConsumerRecord<String, Object> recoveredAudit = KafkaTestRecords.awaitRecord(audit, eventId);
                assertAudit(recoveredAudit, eventId, auditNumber, auditRetry, replay);
                this.assertSignedRequests(eventId, payload, 2);
                assertUncommitted(adminClient, original);
                Assertions.assertThat(KafkaTestRecords.endOffsets(adminClient, DELAYED_TOPICS)).isEqualTo(retryOffsets);
                recoveredAttempt.release();

                PendingAcknowledgement recoveredRetry = publicationAcknowledgements.awaitRetry();
                recoveredRetry.assertSucceededOn(retryTopic);
                ConsumerRecord<String, Object> successor = KafkaTestRecords.awaitRecord(retries, eventId);
                Assertions.assertThat(original.key()).isEqualTo("wrong-inbound-key");
                Assertions.assertThat(successor.key()).isEqualTo(webhookEvent.getEndpointId());
                Assertions.assertThat(successor.value()).isEqualTo(WebhookEvent.newBuilder(webhookEvent)
                        .setDeliveryAttemptCount(auditNumber)
                        .setRetryCount(successorRetry)
                        .setNextEligibleAt(due)
                        .build());
                assertUncommitted(adminClient, original);
                recoveredRetry.release();

                KafkaTestRecords.awaitCommit(adminClient, original);
                publicationAcknowledgements.assertPublicationOrder();
                countRecord(retryOffsets, successor);
                Assertions.assertThat(KafkaTestRecords.endOffsets(adminClient, DELAYED_TOPICS)).isEqualTo(retryOffsets);
                this.assertSignedRequests(eventId, payload, 2);
                Assertions.assertThat(this.attemptRows(eventId)).isEmpty();

                routeController.startRoute(KafkaTestRecords.consumerRoute(ATTEMPT_TOPIC));
                KafkaTestRecords.awaitCommit(adminClient, recoveredAudit);
                Assertions.assertThat(this.attemptRows(eventId)).containsExactly(Map.of(
                        "attempt_number", auditNumber, "retry_count", auditRetry, "replay_count", replay,
                        "outcome", "RETRYABLE_FAILURE"
                ));
            }
            this.assertSignedRequests(eventId, payload, 3);
        }
    }

    private void observeDeliveryCommits(
            String topic,
            String eventId,
            PublicationAcknowledgements publicationAcknowledgements
    ) throws Exception {
        AtomicInteger deliveries = new AtomicInteger();
        AdviceWith.adviceWith(
                this.camelContext,
                KafkaTestRecords.consumerRoute(topic),
                advice -> advice.weaveAddFirst().process(exchange -> {
                    if (eventId.equals(exchange.getMessage().getBody(WebhookEvent.class).getEventId())) {
                        int delivery = deliveries.incrementAndGet();
                        KafkaManualCommit delegate =
                                exchange.getMessage().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
                        KafkaManualCommit observed = () -> {
                            publicationAcknowledgements.observeCommit(delivery);
                            delegate.commit();
                        };
                        exchange.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, observed);
                    }
                })
        );
    }

    private static void countRecord(Map<TopicPartition, Long> offsets, ConsumerRecord<String, Object> consumerRecord) {
        offsets.compute(
                new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                (_, offset) -> offset + 1
        );
    }

    private void startRoutes(String inputTopic) throws Exception {
        this.camelContext.start();
        RouteController controller = this.camelContext.getRouteController();
        for (String routeId : List.of(
                WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT, WebhookRoute.WEBHOOK_DELIVERY_ATTEMPT_DIRECT,
                WebhookRoute.WEBHOOK_RETRY_ELIGIBILITY_DIRECT, KafkaTestRecords.consumerRoute(inputTopic)
        )) {
            controller.startRoute(routeId);
        }
    }

    private static void assertUncommitted(
            AdminClient adminClient,
            ConsumerRecord<String, Object> consumerRecord
    ) throws Exception {
        Assertions.assertThat(KafkaTestRecords.committedOffset(adminClient, consumerRecord))
                .isLessThan(consumerRecord.offset() + 1);
    }

    private static void assertAudit(
            ConsumerRecord<String, Object> consumerRecord,
            String eventId,
            int number,
            int retries,
            int replay
    ) {
        Assertions.assertThat(consumerRecord.key()).isEqualTo(eventId);
        DeliveryAttempt audit = (DeliveryAttempt) consumerRecord.value();
        Assertions.assertThat(audit.getAttemptNumber()).isEqualTo(number);
        Assertions.assertThat(audit.getOutcome()).isEqualTo(DeliveryOutcome.RETRYABLE_FAILURE);
        Assertions.assertThat(audit.getHttpStatus()).isEqualTo(503);
        Assertions.assertThat(audit.getRetryCount()).isEqualTo(retries);
        Assertions.assertThat(audit.getReplayCount()).isEqualTo(replay);
        Assertions.assertThat(audit.getDeadLettered()).isFalse();
        Assertions.assertThat(audit.getSchema().getField("payload")).isNull();
        Assertions.assertThat(audit.getSchema().getField("signingSecret")).isNull();
        Assertions.assertThat(StandardCharsets.UTF_8.decode(audit.getDetails().duplicate()).toString())
                .doesNotContain("private", SIGNING_SECRET);
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

    private void assertSignedRequests(String eventId, byte[] payload, int count) {
        Assertions.assertThat(RECEIVER.requests(eventId)).hasSize(count).allSatisfy(request -> {
            Assertions.assertThat(request.webhookId()).isEqualTo(eventId);
            Assertions.assertThat(request.body()).isEqualTo(payload);
            Assertions.assertThat(request.signature()).isEqualTo(this.signatureComponent.sign(
                    eventId, Long.parseLong(request.timestamp()), payload
            ));
        });
    }

    private enum FailedPublication {
        ATTEMPT, RETRY
    }

    private static final class PublicationAcknowledgements extends DefaultKafkaClientFactory {

        private final String eventId;
        private final int auditNumber;
        private final FailedPublication failure;
        private final BlockingQueue<PendingAcknowledgement> attempts = new LinkedBlockingQueue<>();
        private final BlockingQueue<PendingAcknowledgement> retries = new LinkedBlockingQueue<>();
        private final CompletableFuture<Void> closed = new CompletableFuture<>();
        private final AtomicInteger attemptSends = new AtomicInteger();
        private final AtomicInteger retrySends = new AtomicInteger();
        private final AtomicInteger attemptsAcknowledged = new AtomicInteger();
        private final AtomicBoolean retryAcknowledged = new AtomicBoolean();
        private final AtomicBoolean retriedBeforeAttemptAcknowledgement = new AtomicBoolean();
        private final AtomicBoolean committedBeforeRetryAcknowledgement = new AtomicBoolean();

        PublicationAcknowledgements(String eventId, int auditNumber, FailedPublication failure) {
            this.eventId = eventId;
            this.auditNumber = auditNumber;
            this.failure = failure;
        }

        @Override
        public Producer<Object, Object> getProducer(Properties properties) {
            return new KafkaProducer<>(properties) {
                @Override
                public Future<RecordMetadata> send(ProducerRecord<Object, Object> record, Callback callback) {
                    return PublicationAcknowledgements.this.send(record, callback, super::send);
                }
            };
        }

        private Future<RecordMetadata> send(
                ProducerRecord<Object, Object> producerRecord,
                Callback callback,
                BiFunction<ProducerRecord<Object, Object>, Callback, Future<RecordMetadata>> delegate
        ) {
            return switch (producerRecord.value()) {
                case DeliveryAttempt deliveryAttempt
                        when this.eventId.equals(deliveryAttempt.getEventId())
                        && deliveryAttempt.getAttemptNumber() == this.auditNumber -> {
                    int send = this.attemptSends.incrementAndGet();
                    boolean fail = send == 1 && this.failure == FailedPublication.ATTEMPT;
                    yield this.holdReply(
                            producerRecord,
                            callback,
                            delegate,
                            this.attempts,
                            this.attemptsAcknowledged::incrementAndGet,
                            fail
                    );
                }
                case WebhookEvent webhookEvent
                        when this.eventId.equals(webhookEvent.getEventId())
                        && webhookEvent.getDeliveryAttemptCount() == this.auditNumber -> {
                    int send = this.retrySends.incrementAndGet();
                    if (this.attemptsAcknowledged.get() != send) {
                        this.retriedBeforeAttemptAcknowledgement.set(true);
                    }
                    boolean fail = send == 1 && this.failure == FailedPublication.RETRY;
                    yield this.holdReply(
                            producerRecord,
                            callback,
                            delegate,
                            this.retries,
                            () -> this.retryAcknowledged.set(true),
                            fail
                    );
                }
                default -> delegate.apply(producerRecord, callback);
            };
        }

        private Future<RecordMetadata> holdReply(
                ProducerRecord<Object, Object> producerRecord,
                Callback callback,
                BiFunction<ProducerRecord<Object, Object>, Callback, Future<RecordMetadata>> delegate,
                BlockingQueue<PendingAcknowledgement> blockingQueue,
                Runnable acknowledged,
                boolean fail
        ) {
            PendingAcknowledgement pendingAcknowledgement =
                    new PendingAcknowledgement(callback, acknowledged, this.closed);
            if (fail) {
                pendingAcknowledgement.receive(
                        null,
                        new IllegalStateException("Injected send failure on " + producerRecord.topic())
                );
            } else {
                delegate.apply(producerRecord, pendingAcknowledgement::receive);
            }
            blockingQueue.add(pendingAcknowledgement);
            return pendingAcknowledgement.clientResult;
        }

        PendingAcknowledgement awaitAttempt() throws Exception {
            return awaitReply(this.attempts, "attempt");
        }

        PendingAcknowledgement awaitRetry() throws Exception {
            return awaitReply(this.retries, "retry");
        }

        private static PendingAcknowledgement awaitReply(
                BlockingQueue<PendingAcknowledgement> blockingQueue,
                String name
        ) throws Exception {
            PendingAcknowledgement pendingAcknowledgement = blockingQueue.poll(20, TimeUnit.SECONDS);
            if (pendingAcknowledgement == null) {
                throw new AssertionError("Kafka send did not start: " + name);
            }
            pendingAcknowledgement.brokerReply.get(20, TimeUnit.SECONDS);
            return pendingAcknowledgement;
        }

        void observeCommit(int delivery) {
            if (delivery != 2 || !this.retryAcknowledged.get()) {
                this.committedBeforeRetryAcknowledgement.set(true);
            }
        }

        void assertPublicationOrder() {
            Assertions.assertThat(this.retriedBeforeAttemptAcknowledgement)
                    .as("retry started before the attempt acknowledgement").isFalse();
            Assertions.assertThat(this.committedBeforeRetryAcknowledgement)
                    .as("only recovered delivery may commit, and only after the retry acknowledgement").isFalse();
        }

        void releaseAll() {
            this.closed.complete(null);
        }
    }

    private static final class PendingAcknowledgement {

        private final CompletableFuture<BrokerReply> brokerReply = new CompletableFuture<>();
        private final CompletableFuture<Void> release = new CompletableFuture<>();
        private final CompletableFuture<RecordMetadata> clientResult = new CompletableFuture<>();

        PendingAcknowledgement(Callback callback, Runnable acknowledged, CompletableFuture<Void> closed) {
            // Withhold both Kafka send completion APIs without blocking the producer's I/O thread.
            this.brokerReply.thenAcceptBoth(this.release.applyToEither(closed, _ -> null), (result, _) -> {
                if (result.failure() == null) {
                    acknowledged.run();
                }
                if (callback != null) {
                    callback.onCompletion(result.metadata(), result.failure());
                }
                if (result.failure() == null) {
                    this.clientResult.complete(result.metadata());
                } else {
                    this.clientResult.completeExceptionally(result.failure());
                }
            });
        }

        void receive(RecordMetadata metadata, Exception failure) {
            this.brokerReply.complete(new BrokerReply(metadata, failure));
        }

        void assertSucceeded() {
            Assertions.assertThat(this.brokerReply.join().failure()).isNull();
        }

        void assertSucceededOn(String topic) {
            this.assertSucceeded();
            Assertions.assertThat(this.brokerReply.join().metadata().topic()).isEqualTo(topic);
        }

        void assertFailed() {
            Assertions.assertThat(this.brokerReply.join().failure()).isInstanceOf(IllegalStateException.class);
        }

        void release() {
            this.release.complete(null);
        }
    }

    private record BrokerReply(RecordMetadata metadata, Exception failure) {
    }

    private record CapturedRequest(String webhookId, String timestamp, String signature, byte[] body) {
    }

    private static final class TestReceiver implements AutoCloseable {

        private final HttpServer httpServer;
        private final ExecutorService executorService;
        private final Set<String> failing = ConcurrentHashMap.newKeySet();
        private final Map<String, ConcurrentLinkedQueue<CapturedRequest>> requests = new ConcurrentHashMap<>();

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
            this.failing.add(eventId);
        }

        void succeed(String eventId) {
            this.failing.remove(eventId);
        }

        List<CapturedRequest> requests(String eventId) {
            ConcurrentLinkedQueue<CapturedRequest> matching = this.requests.get(eventId);
            return matching == null ? List.of() : new ArrayList<>(matching);
        }

        private void handle(HttpExchange httpExchange) throws IOException {
            try (httpExchange) {
                byte[] body = httpExchange.getRequestBody().readAllBytes();
                String eventId = httpExchange.getRequestHeaders().getFirst("webhook-id");
                this.requests.computeIfAbsent(eventId, _ -> new ConcurrentLinkedQueue<>()).add(new CapturedRequest(
                        eventId,
                        httpExchange.getRequestHeaders().getFirst("webhook-timestamp"),
                        httpExchange.getRequestHeaders().getFirst("webhook-signature"),
                        body
                ));
                if (!this.failing.contains(eventId)) {
                    httpExchange.sendResponseHeaders(204, -1L);
                } else {
                    byte[] echoed = (new String(body, StandardCharsets.UTF_8) + SIGNING_SECRET)
                            .getBytes(StandardCharsets.UTF_8);
                    httpExchange.sendResponseHeaders(503, echoed.length);
                    httpExchange.getResponseBody().write(echoed);
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
