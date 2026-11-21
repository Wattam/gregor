package gregor.integration;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import gregor.avro.DeliveryAttempt;
import gregor.avro.DeliveryOutcome;
import gregor.avro.WebhookEvent;
import gregor.route.WebhookRoute;
import io.vertx.core.Vertx;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.component.kafka.DefaultKafkaClientFactory;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.spi.RouteController;
import org.apache.camel.spi.Synchronization;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.apache.camel.test.spring.junit5.UseAdviceWith;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
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
    private static final String DEAD_LETTER_TOPIC = "webhook-events-dead-letter";
    private static final String PAYLOAD_MARKER = "terminal-private-payload-marker";
    private static final String[] DELAYED_TOPICS = {WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC, WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC, WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC, DEAD_LETTER_TOPIC};
    private static final String SIGNING_SECRET = "whsec_MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE=";
    private static final TestReceiver TEST_RECEIVER = TestReceiver.start();
    private static final ExpectedAudit SUCCESSFUL_AUDIT = new ExpectedAudit(DeliveryOutcome.SUCCEEDED, 204, false);

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
        boolean fromSourceTopic = Objects.equals(inputTopic, WebhookRoute.WEBHOOK_EVENT_TOPIC);
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
        PublicationAcknowledgements publicationAcknowledgements = new PublicationAcknowledgements(eventId, auditNumber, failedPublication, inputTopic, false, false, false);
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
                if (Objects.equals(failedPublication, FailedPublication.ATTEMPT)) {
                    firstAttemptAcknowledgement.assertFailed();
                    Assertions.assertThat(KafkaTestRecords.endOffsets(adminClient, ATTEMPT_TOPIC)).isEqualTo(attemptOffsets);
                } else {
                    firstAttemptAcknowledgement.assertSucceeded();
                    assertAudit(KafkaTestRecords.awaitRecord(attemptKafkaConsumer, eventId), eventId, auditNumber, auditRetry, replay);
                }
                firstAttemptAcknowledgement.release();

                if (Objects.equals(failedPublication, FailedPublication.RETRY)) {
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
                Assertions.assertThat(this.attemptRows(eventId)).containsExactly(expectedAttemptRow(auditNumber, auditRetry, replay, DeliveryOutcome.RETRYABLE_FAILURE, 503, false));
            }
            this.assertSignedRequests(eventId, payloadBytes, 3);
        }
    }

    @Test
    void repeatsTerminalDeliveryAfterDeadLetterSendFailsBeforeStorage() throws Exception {
        this.withTerminalDelivery(
                TerminalInput.SOURCE,
                FailedPublication.DEAD_LETTER,
                Recovery.REPEAT_TERMINAL,
                terminalDelivery -> {
                    PendingAcknowledgement firstDeadLetterAcknowledgement = terminalDelivery.holdFirstDeadLetter();
                    firstDeadLetterAcknowledgement.assertFailedBeforeStorage();
                    terminalDelivery.assertDeadLetterOffsets();
                    firstDeadLetterAcknowledgement.release();
                    terminalDelivery.assertDeadLetterFailure();

                    terminalDelivery.recoverDeadLetter();
                    ConsumerRecord<String, Object> recoveredAttemptConsumerRecord = terminalDelivery.recoverAudit(terminalDelivery.terminalAudit);
                    terminalDelivery.assertPersistedAudit(recoveredAttemptConsumerRecord, terminalDelivery.terminalAudit);
                }
        );
    }

    @Test
    void succeedsWithoutADeadLetterAfterDeadLetterSendFailsBeforeStorage() throws Exception {
        this.withTerminalDelivery(
                TerminalInput.SOURCE,
                FailedPublication.DEAD_LETTER,
                Recovery.SUCCEED,
                terminalDelivery -> {
                    PendingAcknowledgement firstDeadLetterAcknowledgement = terminalDelivery.holdFirstDeadLetter();
                    firstDeadLetterAcknowledgement.assertFailedBeforeStorage();
                    terminalDelivery.assertDeadLetterOffsets();
                    TEST_RECEIVER.succeed(terminalDelivery.eventId());
                    firstDeadLetterAcknowledgement.release();
                    terminalDelivery.assertDeadLetterFailure();

                    ConsumerRecord<String, Object> recoveredAttemptConsumerRecord = terminalDelivery.recoverAudit(SUCCESSFUL_AUDIT);
                    terminalDelivery.assertPersistedAudit(recoveredAttemptConsumerRecord, SUCCESSFUL_AUDIT);
                }
        );
    }

    @Test
    void retainsTwoDeadLettersAfterLostDeadLetterAcknowledgementAndSourceRestart() throws Exception {
        this.withTerminalDelivery(
                TerminalInput.SOURCE,
                FailedPublication.LOST_DEAD_LETTER_ACK,
                Recovery.RESTART_AND_REPEAT_TERMINAL,
                terminalDelivery -> {
                    PendingAcknowledgement firstDeadLetterAcknowledgement = terminalDelivery.holdFirstDeadLetter();
                    firstDeadLetterAcknowledgement.assertStoredButFailedOn(DEAD_LETTER_TOPIC);
                    ConsumerRecord<String, Object> firstDeadLetterConsumerRecord = terminalDelivery.readDeadLetter();
                    firstDeadLetterAcknowledgement.release();
                    terminalDelivery.assertDeadLetterFailure();
                    terminalDelivery.restartInputConsumer(0);

                    assertLaterRecord(firstDeadLetterConsumerRecord, terminalDelivery.recoverDeadLetter());
                    ConsumerRecord<String, Object> recoveredAttemptConsumerRecord = terminalDelivery.recoverAudit(terminalDelivery.terminalAudit);
                    terminalDelivery.assertPersistedAudit(recoveredAttemptConsumerRecord, terminalDelivery.terminalAudit);
                }
        );
    }

    @Test
    void retainsTwoDeadLettersAfterTerminalAuditSendFailsAndRetryConsumerRestarts() throws Exception {
        this.withTerminalDelivery(
                TerminalInput.LAST_RETRY,
                FailedPublication.ATTEMPT,
                Recovery.RESTART_AND_REPEAT_TERMINAL,
                terminalDelivery -> {
                    PendingAcknowledgement firstDeadLetterAcknowledgement = terminalDelivery.holdFirstDeadLetter();
                    firstDeadLetterAcknowledgement.assertSucceededOn(DEAD_LETTER_TOPIC);
                    ConsumerRecord<String, Object> firstDeadLetterConsumerRecord = terminalDelivery.readDeadLetter();
                    firstDeadLetterAcknowledgement.release();

                    PendingAcknowledgement firstAttemptAcknowledgement = terminalDelivery.holdAttempt();
                    firstAttemptAcknowledgement.assertFailedBeforeStorage();
                    terminalDelivery.assertNoStoredAttempts();
                    firstAttemptAcknowledgement.release();
                    terminalDelivery.assertAuditFailure();
                    terminalDelivery.restartInputConsumer(1);

                    assertLaterRecord(firstDeadLetterConsumerRecord, terminalDelivery.recoverDeadLetter());
                    ConsumerRecord<String, Object> recoveredAttemptConsumerRecord = terminalDelivery.recoverAudit(terminalDelivery.terminalAudit);
                    terminalDelivery.assertPersistedAudit(recoveredAttemptConsumerRecord, terminalDelivery.terminalAudit);
                }
        );
    }

    @Test
    void succeedsButKeepsTheFirstDeadLetterAndAuditAfterLostAuditAcknowledgement() throws Exception {
        this.withTerminalDelivery(
                TerminalInput.LAST_RETRY,
                FailedPublication.LOST_ATTEMPT_ACK,
                Recovery.SUCCEED,
                terminalDelivery -> {
                    PendingAcknowledgement firstDeadLetterAcknowledgement = terminalDelivery.holdFirstDeadLetter();
                    firstDeadLetterAcknowledgement.assertSucceededOn(DEAD_LETTER_TOPIC);
                    ConsumerRecord<String, Object> firstDeadLetterConsumerRecord = terminalDelivery.readDeadLetter();
                    firstDeadLetterAcknowledgement.release();

                    PendingAcknowledgement firstAttemptAcknowledgement = terminalDelivery.holdAttempt();
                    firstAttemptAcknowledgement.assertStoredButFailedOn(ATTEMPT_TOPIC);
                    ConsumerRecord<String, Object> firstAttemptConsumerRecord = terminalDelivery.readAudit(terminalDelivery.terminalAudit);
                    TEST_RECEIVER.succeed(terminalDelivery.eventId());
                    firstAttemptAcknowledgement.release();
                    terminalDelivery.assertAuditFailure();

                    ConsumerRecord<String, Object> recoveredAttemptConsumerRecord = terminalDelivery.recoverAudit(SUCCESSFUL_AUDIT);
                    terminalDelivery.assertDeadLetterStillRetained(firstDeadLetterConsumerRecord);
                    assertLaterRecord(firstAttemptConsumerRecord, recoveredAttemptConsumerRecord);
                    terminalDelivery.assertPersistedAudit(recoveredAttemptConsumerRecord, terminalDelivery.terminalAudit);
                }
        );
    }

    private void withTerminalDelivery(TerminalInput terminalInput, FailedPublication failedPublication, Recovery recovery, TerminalScenario terminalScenario) throws Exception {
        AtomicReference<Instant> currentInstantReference = new AtomicReference<>(NOW);
        Mockito.when(this.clock.instant()).thenAnswer(_ -> currentInstantReference.get());
        WebhookEvent inputWebhookEvent = terminalInput.newEvent();
        String eventId = inputWebhookEvent.getEventId();
        String inputTopic = terminalInput.topic();
        int attemptNumber = inputWebhookEvent.getDeliveryAttemptCount() + 1;
        PublicationAcknowledgements publicationAcknowledgements = new PublicationAcknowledgements(
                eventId,
                attemptNumber,
                failedPublication,
                inputTopic,
                true,
                Objects.equals(recovery, Recovery.SUCCEED),
                Objects.equals(recovery, Recovery.RESTART_AND_REPEAT_TERMINAL)
        );
        this.camelContext.getComponent("kafka", KafkaComponent.class).setKafkaClientFactory(publicationAcknowledgements);
        this.observeDeliveryCommits(inputTopic, eventId, publicationAcknowledgements);
        this.startRoutes(inputTopic);
        RouteController routeController = this.camelContext.getRouteController();
        String inputRoute = KafkaTestRecords.consumerRoute(inputTopic);

        try (AdminClient adminClient = KafkaTestRecords.adminClient()) {
            TopicPartition inputTopicPartition = new TopicPartition(inputTopic, 0);
            Map<TopicPartition, Long> inputOffsets = KafkaTestRecords.endOffsets(adminClient, inputTopic);
            long inputOffset = inputOffsets.get(inputTopicPartition);
            Map<TopicPartition, Long> retryOffsets = KafkaTestRecords.retryEndOffsets(adminClient);
            TerminalOffsets terminalOffsets = new TerminalOffsets(KafkaTestRecords.endOffsets(adminClient, ATTEMPT_TOPIC), new HashMap<>(retryOffsets), new HashMap<>(KafkaTestRecords.endOffsets(adminClient, DEAD_LETTER_TOPIC)));
            try (
                    AutoCloseable _ = () -> {
                        TEST_RECEIVER.succeed(eventId);
                        publicationAcknowledgements.releaseAll();
                        try {
                            if (routeController.getRouteStatus(inputRoute).isStopped()) {
                                routeController.startRoute(inputRoute);
                            }
                            if (KafkaTestRecords.endOffsets(adminClient, inputTopic).get(inputTopicPartition) > inputOffset) {
                                KafkaTestRecords.awaitCommit(adminClient, inputTopicPartition, inputOffset);
                            }
                        } finally {
                            KafkaTestRecords.drainRetries(adminClient, routeController, retryOffsets, currentInstantReference);
                        }
                    };
                    KafkaConsumer<String, Object> inputKafkaConsumer = KafkaTestRecords.consumer(this.registryVertx, inputOffsets);
                    KafkaConsumer<String, Object> attemptKafkaConsumer = KafkaTestRecords.consumer(this.registryVertx, terminalOffsets.attemptOffsets());
                    KafkaConsumer<String, Object> deadLetterKafkaConsumer = KafkaTestRecords.consumer(this.registryVertx, terminalOffsets.deadLetterOffsets())
            ) {
                TEST_RECEIVER.fail(eventId, terminalInput.audit().httpStatus());
                this.producerTemplate.sendBodyAndHeaders(
                        "kafka:" + inputTopic,
                        inputWebhookEvent,
                        Map.of(KafkaConstants.KEY, "wrong-inbound-key", KafkaConstants.PARTITION_KEY, 0, "source-metadata", "discard", "webhook-signature", "discard", "signing-secret", "discard")
                );
                ConsumerRecord<String, Object> inputConsumerRecord = KafkaTestRecords.awaitRecord(inputKafkaConsumer, eventId);
                Assertions.assertThat(inputConsumerRecord.key()).isEqualTo("wrong-inbound-key");
                Assertions.assertThat(inputConsumerRecord.headers().lastHeader("source-metadata")).isNotNull();
                if (Objects.equals(terminalInput, TerminalInput.LAST_RETRY)) {
                    countRecord(terminalOffsets.retryOffsets(), inputConsumerRecord);
                }

                terminalScenario.run(new TerminalDelivery(
                        adminClient,
                        inputConsumerRecord,
                        WebhookEvent.newBuilder(inputWebhookEvent).setDeliveryAttemptCount(attemptNumber).build(),
                        terminalInput.audit(),
                        publicationAcknowledgements,
                        terminalOffsets,
                        attemptKafkaConsumer,
                        deadLetterKafkaConsumer
                ));
            }
        }
    }

    private static void assertLaterRecord(ConsumerRecord<String, Object> firstConsumerRecord, ConsumerRecord<String, Object> laterConsumerRecord) {
        Assertions.assertThat(laterConsumerRecord.partition()).isEqualTo(firstConsumerRecord.partition());
        Assertions.assertThat(laterConsumerRecord.offset()).isGreaterThan(firstConsumerRecord.offset());
    }

    private static ConsumerRecord<String, Object> assertRetainedDeadLetter(KafkaConsumer<String, Object> deadLetterKafkaConsumer, WebhookEvent expectedWebhookEvent) {
        ConsumerRecord<String, Object> consumerRecord = KafkaTestRecords.awaitRecord(deadLetterKafkaConsumer, expectedWebhookEvent.getEventId());
        Assertions.assertThat(consumerRecord.topic()).isEqualTo(DEAD_LETTER_TOPIC);
        Assertions.assertThat(consumerRecord.key()).isEqualTo(expectedWebhookEvent.getEndpointId());
        Assertions.assertThat(consumerRecord.value()).isEqualTo(expectedWebhookEvent);
        Assertions.assertThat(consumerRecord.headers()).allSatisfy(header -> Assertions.assertThat(header.key()).startsWith("apicurio."));
        return consumerRecord;
    }

    private static void assertSanitizedDeadLetterFailure(Throwable throwable) {
        Throwable cause = rootCause(throwable);
        Assertions.assertThat(cause).isInstanceOf(IllegalStateException.class).hasMessage("Dead-letter publication failed [topic=webhook-events-dead-letter]");
        Assertions.assertThat(cause.getSuppressed()).isEmpty();
        StringWriter stringWriter = new StringWriter();
        throwable.printStackTrace(new PrintWriter(stringWriter));
        Assertions.assertThat(stringWriter.toString()).doesNotContain(PAYLOAD_MARKER, SIGNING_SECRET);
    }

    private static Throwable rootCause(Throwable throwable) {
        Throwable cause = throwable;
        while (Objects.nonNull(cause.getCause())) {
            cause = cause.getCause();
        }
        return cause;
    }

    private void observeDeliveryCommits(String topic, String eventId, PublicationAcknowledgements publicationAcknowledgements) throws Exception {
        AtomicInteger deliveryCounter = new AtomicInteger();
        AdviceWith.adviceWith(
                this.camelContext,
                KafkaTestRecords.consumerRoute(topic),
                adviceWithRouteBuilder -> adviceWithRouteBuilder.weaveAddFirst().process(exchange -> {
                    if (Objects.equals(eventId, exchange.getMessage().getBody(WebhookEvent.class).getEventId())) {
                        int delivery = deliveryCounter.incrementAndGet();
                        if (delivery == 1 && publicationAcknowledgements.terminal) {
                            exchange.getUnitOfWork().addSynchronization(new Synchronization() {
                                @Override
                                public void onComplete(@NonNull Exchange exchange) {
                                    publicationAcknowledgements.firstFailureFuture.completeExceptionally(new AssertionError("The first delivery unexpectedly succeeded"));
                                }

                                @Override
                                public void onFailure(@NonNull Exchange exchange) {
                                    publicationAcknowledgements.firstFailureFuture.complete(exchange.getException());
                                }
                            });
                        }
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
        assertAudit(consumerRecord, eventId, number, retries, replay, DeliveryOutcome.RETRYABLE_FAILURE, 503, false);
    }

    private static void assertAudit(ConsumerRecord<String, Object> consumerRecord, String eventId, int number, int retries, int replay, DeliveryOutcome deliveryOutcome, int httpStatus, boolean deadLettered) {
        Assertions.assertThat(consumerRecord.key()).isEqualTo(eventId);
        DeliveryAttempt deliveryAttempt = (DeliveryAttempt) consumerRecord.value();
        Assertions.assertThat(deliveryAttempt.getAttemptNumber()).isEqualTo(number);
        Assertions.assertThat(deliveryAttempt.getOutcome()).isEqualTo(deliveryOutcome);
        Assertions.assertThat(deliveryAttempt.getHttpStatus()).isEqualTo(httpStatus);
        Assertions.assertThat(deliveryAttempt.getRetryCount()).isEqualTo(retries);
        Assertions.assertThat(deliveryAttempt.getReplayCount()).isEqualTo(replay);
        Assertions.assertThat(deliveryAttempt.getDeadLettered()).isEqualTo(deadLettered);
        Assertions.assertThat(deliveryAttempt.getSchema().getField("payload")).isNull();
        Assertions.assertThat(deliveryAttempt.getSchema().getField("signingSecret")).isNull();
        Assertions.assertThat(StandardCharsets.UTF_8.decode(deliveryAttempt.getDetails().duplicate()).toString()).doesNotContain("private", PAYLOAD_MARKER, SIGNING_SECRET);
    }

    private static Map<String, Object> expectedAttemptRow(int number, int retries, int replay, DeliveryOutcome deliveryOutcome, int httpStatus, boolean deadLettered) {
        return Map.of("attempt_number", number, "retry_count", retries, "replay_count", replay, "outcome", deliveryOutcome.name(), "http_status", httpStatus, "dead_lettered", deadLettered);
    }

    private List<Map<String, Object>> attemptRows(String eventId) {
        return this.jdbcTemplate.queryForList(
                """
                SELECT attempt_number, retry_count, replay_count, outcome, http_status, dead_lettered
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

    @FunctionalInterface
    private interface TerminalScenario {

        void run(TerminalDelivery terminalDelivery) throws Exception;
    }

    private enum Recovery {
        REPEAT_TERMINAL, RESTART_AND_REPEAT_TERMINAL, SUCCEED
    }

    private record TerminalInput(String topic, int previousAttempts, int retryCount, int replayCount, Instant nextEligibleAt, ExpectedAudit audit) {

        private static final TerminalInput SOURCE = new TerminalInput(WebhookRoute.WEBHOOK_EVENT_TOPIC, 0, 0, 0, null, new ExpectedAudit(DeliveryOutcome.PERMANENT_FAILURE, 400, true));
        private static final TerminalInput LAST_RETRY = new TerminalInput(WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC, 3, 3, 2, Instant.parse("2026-07-25T11:55:00Z"), new ExpectedAudit(DeliveryOutcome.RETRYABLE_FAILURE, 503, true));

        WebhookEvent newEvent() {
            byte[] payloadBytes = ("{ \"private\": \"" + PAYLOAD_MARKER + " é\" }").getBytes(StandardCharsets.UTF_8);
            return new WebhookEvent(
                    UUID.randomUUID().toString(),
                    "terminal-publication.test",
                    "00000000-0000-0000-0000-000000000001",
                    "00000000-0000-0000-0000-000000000002",
                    Instant.parse("2026-07-25T11:00:00Z"),
                    ByteBuffer.wrap(payloadBytes),
                    this.retryCount,
                    this.nextEligibleAt,
                    this.previousAttempts,
                    this.replayCount
            );
        }
    }

    private record ExpectedAudit(DeliveryOutcome deliveryOutcome, int httpStatus, boolean deadLettered) {
    }

    private record TerminalOffsets(Map<TopicPartition, Long> attemptOffsets, Map<TopicPartition, Long> retryOffsets, Map<TopicPartition, Long> deadLetterOffsets) {
    }

    @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
    private final class TerminalDelivery {

        private final AdminClient adminClient;
        private final ConsumerRecord<String, Object> inputConsumerRecord;
        private final WebhookEvent expectedDeadLetterWebhookEvent;
        private final ExpectedAudit terminalAudit;
        private final PublicationAcknowledgements publicationAcknowledgements;
        private final TerminalOffsets terminalOffsets;
        private final KafkaConsumer<String, Object> attemptKafkaConsumer;
        private final KafkaConsumer<String, Object> deadLetterKafkaConsumer;

        String eventId() {
            return this.expectedDeadLetterWebhookEvent.getEventId();
        }

        PendingAcknowledgement holdFirstDeadLetter() throws Exception {
            PendingAcknowledgement pendingAcknowledgement = this.publicationAcknowledgements.awaitDeadLetter();
            this.assertRequests(1);
            assertUncommitted(this.adminClient, this.inputConsumerRecord);
            this.publicationAcknowledgements.assertAttemptSends(0);
            this.assertNoStoredAttempts();
            return pendingAcknowledgement;
        }

        ConsumerRecord<String, Object> readDeadLetter() throws Exception {
            ConsumerRecord<String, Object> deadLetterConsumerRecord = assertRetainedDeadLetter(this.deadLetterKafkaConsumer, this.expectedDeadLetterWebhookEvent);
            countRecord(this.terminalOffsets.deadLetterOffsets(), deadLetterConsumerRecord);
            this.assertDeadLetterOffsets();
            return deadLetterConsumerRecord;
        }

        void assertDeadLetterOffsets() throws Exception {
            Assertions.assertThat(KafkaTestRecords.endOffsets(this.adminClient, DEAD_LETTER_TOPIC)).isEqualTo(this.terminalOffsets.deadLetterOffsets());
        }

        void assertDeadLetterFailure() throws Exception {
            assertSanitizedDeadLetterFailure(this.publicationAcknowledgements.awaitFirstFailure());
        }

        PendingAcknowledgement holdAttempt() throws Exception {
            PendingAcknowledgement pendingAcknowledgement = this.publicationAcknowledgements.awaitAttempt();
            assertUncommitted(this.adminClient, this.inputConsumerRecord);
            return pendingAcknowledgement;
        }

        ConsumerRecord<String, Object> readAudit(ExpectedAudit expectedAudit) {
            ConsumerRecord<String, Object> attemptConsumerRecord = KafkaTestRecords.awaitRecord(this.attemptKafkaConsumer, this.eventId());
            WebhookEvent webhookEvent = this.expectedDeadLetterWebhookEvent;
            assertAudit(
                    attemptConsumerRecord,
                    this.eventId(),
                    webhookEvent.getDeliveryAttemptCount(),
                    webhookEvent.getRetryCount(),
                    webhookEvent.getReplayCount(),
                    expectedAudit.deliveryOutcome(),
                    expectedAudit.httpStatus(),
                    expectedAudit.deadLettered()
            );
            return attemptConsumerRecord;
        }

        void assertNoStoredAttempts() throws Exception {
            Assertions.assertThat(KafkaTestRecords.endOffsets(this.adminClient, ATTEMPT_TOPIC)).isEqualTo(this.terminalOffsets.attemptOffsets());
        }

        void assertAuditFailure() throws Exception {
            Assertions.assertThat(rootCause(this.publicationAcknowledgements.awaitFirstFailure())).hasMessage("Injected send failure on " + ATTEMPT_TOPIC);
        }

        void restartInputConsumer(int expectedAttemptSends) throws Exception {
            this.publicationAcknowledgements.awaitPausedPoll();
            assertUncommitted(this.adminClient, this.inputConsumerRecord);
            this.publicationAcknowledgements.assertAttemptSends(expectedAttemptSends);
            RouteController routeController = DeliveryPublicationRedeliveryIntegrationTests.this.camelContext.getRouteController();
            String inputRoute = KafkaTestRecords.consumerRoute(this.inputConsumerRecord.topic());
            Assertions.assertThat(routeController.stopRoute(inputRoute, 5, TimeUnit.SECONDS, false)).isTrue();
            assertUncommitted(this.adminClient, this.inputConsumerRecord);
            this.assertRequests(1);
            routeController.startRoute(inputRoute);
        }

        ConsumerRecord<String, Object> recoverDeadLetter() throws Exception {
            PendingAcknowledgement recoveredDeadLetterAcknowledgement = this.publicationAcknowledgements.awaitDeadLetter();
            recoveredDeadLetterAcknowledgement.assertSucceededOn(DEAD_LETTER_TOPIC);
            this.assertRequests(2);
            assertUncommitted(this.adminClient, this.inputConsumerRecord);
            ConsumerRecord<String, Object> recoveredDeadLetterConsumerRecord = this.readDeadLetter();
            recoveredDeadLetterAcknowledgement.release();
            return recoveredDeadLetterConsumerRecord;
        }

        ConsumerRecord<String, Object> recoverAudit(ExpectedAudit expectedAudit) throws Exception {
            PendingAcknowledgement recoveredAttemptAcknowledgement = this.holdAttempt();
            recoveredAttemptAcknowledgement.assertSucceededOn(ATTEMPT_TOPIC);
            ConsumerRecord<String, Object> recoveredAttemptConsumerRecord = this.readAudit(expectedAudit);
            Assertions.assertThat(DeliveryPublicationRedeliveryIntegrationTests.this.attemptRows(this.eventId())).isEmpty();
            recoveredAttemptAcknowledgement.release();

            KafkaTestRecords.awaitCommit(this.adminClient, this.inputConsumerRecord);
            this.publicationAcknowledgements.assertPublicationOrder();
            this.assertRequests(2);
            this.assertDeadLetterOffsets();
            Assertions.assertThat(KafkaTestRecords.retryEndOffsets(this.adminClient)).isEqualTo(this.terminalOffsets.retryOffsets());
            Assertions.assertThat(DeliveryPublicationRedeliveryIntegrationTests.this.attemptRows(this.eventId())).isEmpty();
            return recoveredAttemptConsumerRecord;
        }

        void assertDeadLetterStillRetained(ConsumerRecord<String, Object> deadLetterConsumerRecord) {
            this.deadLetterKafkaConsumer.seek(new TopicPartition(DEAD_LETTER_TOPIC, deadLetterConsumerRecord.partition()), deadLetterConsumerRecord.offset());
            Assertions.assertThat(assertRetainedDeadLetter(this.deadLetterKafkaConsumer, this.expectedDeadLetterWebhookEvent).offset()).isEqualTo(deadLetterConsumerRecord.offset());
        }

        void assertPersistedAudit(ConsumerRecord<String, Object> recoveredAttemptConsumerRecord, ExpectedAudit expectedAudit) throws Exception {
            DeliveryPublicationRedeliveryIntegrationTests.this.camelContext.getRouteController().startRoute(KafkaTestRecords.consumerRoute(ATTEMPT_TOPIC));
            KafkaTestRecords.awaitCommit(this.adminClient, recoveredAttemptConsumerRecord);
            WebhookEvent webhookEvent = this.expectedDeadLetterWebhookEvent;
            Assertions.assertThat(DeliveryPublicationRedeliveryIntegrationTests.this.attemptRows(this.eventId()))
                    .containsExactly(expectedAttemptRow(
                            webhookEvent.getDeliveryAttemptCount(),
                            webhookEvent.getRetryCount(),
                            webhookEvent.getReplayCount(),
                            expectedAudit.deliveryOutcome(),
                            expectedAudit.httpStatus(),
                            expectedAudit.deadLettered()
                    ));
        }

        private void assertRequests(int count) {
            ByteBuffer payloadByteBuffer = this.expectedDeadLetterWebhookEvent.getPayload().duplicate();
            byte[] payloadBytes = new byte[payloadByteBuffer.remaining()];
            payloadByteBuffer.get(payloadBytes);
            DeliveryPublicationRedeliveryIntegrationTests.this.assertSignedRequests(this.eventId(), payloadBytes, count);
        }
    }

    private enum FailedPublication {
        ATTEMPT, RETRY, DEAD_LETTER, LOST_DEAD_LETTER_ACK, LOST_ATTEMPT_ACK
    }

    private enum SendFailure {
        NONE, BEFORE_STORAGE, AFTER_STORAGE
    }

    private static final class PublicationAcknowledgements extends DefaultKafkaClientFactory {

        private final String eventId;
        private final int auditNumber;
        private final FailedPublication failedPublication;
        private final String inputTopic;
        private final boolean terminal;
        private final boolean succeedsOnRedelivery;
        private final BlockingQueue<PendingAcknowledgement> attemptAcknowledgements = new LinkedBlockingQueue<>();
        private final BlockingQueue<PendingAcknowledgement> retryAcknowledgements = new LinkedBlockingQueue<>();
        private final BlockingQueue<PendingAcknowledgement> deadLetterAcknowledgements = new LinkedBlockingQueue<>();
        private final CompletableFuture<Void> closedFuture = new CompletableFuture<>();
        private final CompletableFuture<Throwable> firstFailureFuture = new CompletableFuture<>();
        private final CompletableFuture<Void> pausedPollFuture = new CompletableFuture<>();
        private final CompletableFuture<Void> resumePollFuture = new CompletableFuture<>();
        private final AtomicBoolean restartPauseArmed;
        private final AtomicInteger attemptSends = new AtomicInteger();
        private final AtomicInteger retrySends = new AtomicInteger();
        private final AtomicInteger deadLetterSends = new AtomicInteger();
        private final AtomicInteger attemptsAcknowledged = new AtomicInteger();
        private final AtomicInteger deadLettersAcknowledged = new AtomicInteger();
        private final AtomicBoolean retryAcknowledged = new AtomicBoolean();
        private final AtomicBoolean retriedBeforeAttemptAcknowledgement = new AtomicBoolean();
        private final AtomicBoolean attemptedBeforeDeadLetterAcknowledgement = new AtomicBoolean();
        private final AtomicBoolean committedBeforeRequiredAcknowledgements = new AtomicBoolean();

        PublicationAcknowledgements(String eventId, int auditNumber, FailedPublication failedPublication, String inputTopic, boolean terminal, boolean succeedsOnRedelivery, boolean restartBeforeRedelivery) {
            this.eventId = eventId;
            this.auditNumber = auditNumber;
            this.failedPublication = failedPublication;
            this.inputTopic = inputTopic;
            this.terminal = terminal;
            this.succeedsOnRedelivery = succeedsOnRedelivery;
            this.restartPauseArmed = new AtomicBoolean(restartBeforeRedelivery);
        }

        @Override
        public KafkaConsumer<Object, Object> getConsumer(Properties properties) {
            if (!this.restartPauseArmed.get() || !Objects.equals(this.inputTopic, properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG))) {
                return new KafkaConsumer<>(properties);
            }
            return new KafkaConsumer<>(properties) {
                @Override
                public ConsumerRecords<Object, Object> poll(Duration duration) {
                    if (PublicationAcknowledgements.this.firstFailureFuture.isDone() && PublicationAcknowledgements.this.restartPauseArmed.compareAndSet(true, false)) {
                        // Prevent this consumer from fetching the failed input again before the route restarts.
                        PublicationAcknowledgements.this.pausedPollFuture.complete(null);
                        PublicationAcknowledgements.this.resumePollFuture.orTimeout(30, TimeUnit.SECONDS).join();
                    }
                    return super.poll(duration);
                }

                @Override
                public void wakeup() {
                    super.wakeup();
                    PublicationAcknowledgements.this.resumePollFuture.complete(null);
                }
            };
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
                case DeliveryAttempt deliveryAttempt when Objects.equals(this.eventId, deliveryAttempt.getEventId()) && deliveryAttempt.getAttemptNumber() == this.auditNumber -> {
                    int send = this.attemptSends.incrementAndGet();
                    boolean recoveredSuccess = this.succeedsOnRedelivery && Objects.equals(deliveryAttempt.getOutcome(), DeliveryOutcome.SUCCEEDED);
                    if (this.terminal && !recoveredSuccess && this.deadLettersAcknowledged.get() != send) {
                        this.attemptedBeforeDeadLetterAcknowledgement.set(true);
                    }
                    SendFailure sendFailure = send != 1 ? SendFailure.NONE : switch (this.failedPublication) {
                        case ATTEMPT -> SendFailure.BEFORE_STORAGE;
                        case LOST_ATTEMPT_ACK -> SendFailure.AFTER_STORAGE;
                        default -> SendFailure.NONE;
                    };
                    yield this.holdReply(producerRecord, callback, sendFunction, this.attemptAcknowledgements, this.attemptsAcknowledged::incrementAndGet, sendFailure);
                }
                case WebhookEvent webhookEvent when Objects.equals(this.eventId, webhookEvent.getEventId()) && webhookEvent.getDeliveryAttemptCount() == this.auditNumber -> {
                    if (Objects.equals(DEAD_LETTER_TOPIC, producerRecord.topic())) {
                        int send = this.deadLetterSends.incrementAndGet();
                        SendFailure sendFailure = send != 1 ? SendFailure.NONE : switch (this.failedPublication) {
                            case DEAD_LETTER -> SendFailure.BEFORE_STORAGE;
                            case LOST_DEAD_LETTER_ACK -> SendFailure.AFTER_STORAGE;
                            default -> SendFailure.NONE;
                        };
                        yield this.holdReply(producerRecord, callback, sendFunction, this.deadLetterAcknowledgements, this.deadLettersAcknowledged::incrementAndGet, sendFailure);
                    }
                    int send = this.retrySends.incrementAndGet();
                    if (this.attemptsAcknowledged.get() != send) {
                        this.retriedBeforeAttemptAcknowledgement.set(true);
                    }
                    SendFailure sendFailure = send == 1 && Objects.equals(this.failedPublication, FailedPublication.RETRY) ? SendFailure.BEFORE_STORAGE : SendFailure.NONE;
                    yield this.holdReply(producerRecord, callback, sendFunction, this.retryAcknowledgements, () -> this.retryAcknowledged.set(true), sendFailure);
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
                SendFailure sendFailure
        ) {
            PendingAcknowledgement pendingAcknowledgement = new PendingAcknowledgement(callback, onAcknowledged, this.closedFuture);
            String message = "Injected send failure on " + producerRecord.topic();
            if (Objects.equals(DEAD_LETTER_TOPIC, producerRecord.topic())) {
                message += " [payload=" + PAYLOAD_MARKER + "] [secret=" + SIGNING_SECRET + "]";
            }
            IllegalStateException injectedException = Objects.equals(sendFailure, SendFailure.NONE) ? null : new IllegalStateException(message);
            switch (sendFailure) {
                case BEFORE_STORAGE -> pendingAcknowledgement.receive(null, injectedException);
                case AFTER_STORAGE -> sendFunction.apply(producerRecord, (recordMetadata, exception) -> pendingAcknowledgement.receive(recordMetadata, Objects.isNull(exception) ? injectedException : exception));
                case NONE -> sendFunction.apply(producerRecord, pendingAcknowledgement::receive);
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

        PendingAcknowledgement awaitDeadLetter() throws Exception {
            return awaitReply(this.deadLetterAcknowledgements, "dead letter");
        }

        Throwable awaitFirstFailure() throws Exception {
            return this.firstFailureFuture.get(20, TimeUnit.SECONDS);
        }

        void awaitPausedPoll() throws Exception {
            this.pausedPollFuture.get(20, TimeUnit.SECONDS);
        }

        void assertAttemptSends(int expectedSends) {
            Assertions.assertThat(this.attemptSends).hasValue(expectedSends);
        }

        private static PendingAcknowledgement awaitReply(BlockingQueue<PendingAcknowledgement> pendingAcknowledgements, String name) throws Exception {
            PendingAcknowledgement pendingAcknowledgement = pendingAcknowledgements.poll(20, TimeUnit.SECONDS);
            if (Objects.isNull(pendingAcknowledgement)) {
                throw new AssertionError("Kafka send did not start: " + name);
            }
            pendingAcknowledgement.brokerReplyFuture.get(20, TimeUnit.SECONDS);
            return pendingAcknowledgement;
        }

        void observeCommit(int delivery) {
            int expectedDeadLetterAcknowledgements = switch (this.failedPublication) {
                case ATTEMPT -> 2;
                case DEAD_LETTER -> this.succeedsOnRedelivery ? 0 : 1;
                default -> 1;
            };
            boolean acknowledged = this.terminal ? this.attemptsAcknowledged.get() == 1 && this.deadLettersAcknowledged.get() == expectedDeadLetterAcknowledgements : this.retryAcknowledged.get();
            if (delivery != 2 || !acknowledged) {
                this.committedBeforeRequiredAcknowledgements.set(true);
            }
        }

        void assertPublicationOrder() {
            Assertions.assertThat(this.retriedBeforeAttemptAcknowledgement).as("retry started before the attempt acknowledgement").isFalse();
            Assertions.assertThat(this.attemptedBeforeDeadLetterAcknowledgement).as("terminal audit started before the dead-letter acknowledgement").isFalse();
            Assertions.assertThat(this.committedBeforeRequiredAcknowledgements).as("only recovered delivery may commit, and only after required acknowledgements").isFalse();
        }

        void releaseAll() {
            this.closedFuture.complete(null);
            this.resumePollFuture.complete(null);
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
                        if (Objects.isNull(brokerReply.exception())) {
                            onAcknowledged.run();
                        }
                        if (Objects.nonNull(callback)) {
                            callback.onCompletion(brokerReply.recordMetadata(), brokerReply.exception());
                        }
                        if (Objects.isNull(brokerReply.exception())) {
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

        void assertFailedBeforeStorage() {
            this.assertFailed();
            Assertions.assertThat(this.brokerReplyFuture.join().recordMetadata()).isNull();
        }

        void assertStoredButFailedOn(String topic) {
            this.assertFailed();
            Assertions.assertThat(this.brokerReplyFuture.join().recordMetadata().topic()).isEqualTo(topic);
        }

        void release() {
            this.releaseFuture.complete(null);
        }
    }

    private record BrokerReply(RecordMetadata recordMetadata, Exception exception) {
    }

    private record CapturedRequest(String webhookId, String timestamp, String signature, byte[] bodyBytes) {
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static final class TestReceiver implements AutoCloseable {

        private final HttpServer httpServer;
        private final ExecutorService executorService;
        private final Map<String, Integer> failureStatusesByEventId = new ConcurrentHashMap<>();
        private final Map<String, ConcurrentLinkedQueue<CapturedRequest>> capturedRequestsByEventId = new ConcurrentHashMap<>();

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
            this.fail(eventId, 503);
        }

        void fail(String eventId, int httpStatus) {
            this.failureStatusesByEventId.put(eventId, httpStatus);
        }

        void succeed(String eventId) {
            this.failureStatusesByEventId.remove(eventId);
        }

        List<CapturedRequest> requests(String eventId) {
            ConcurrentLinkedQueue<CapturedRequest> matchingCapturedRequests = this.capturedRequestsByEventId.get(eventId);
            return Objects.isNull(matchingCapturedRequests) ? List.of() : new ArrayList<>(matchingCapturedRequests);
        }

        private void handle(HttpExchange httpExchange) throws IOException {
            try (httpExchange) {
                byte[] bodyBytes = httpExchange.getRequestBody().readAllBytes();
                String eventId = httpExchange.getRequestHeaders().getFirst("webhook-id");
                this.capturedRequestsByEventId
                        .computeIfAbsent(eventId, _ -> new ConcurrentLinkedQueue<>())
                        .add(new CapturedRequest(eventId, httpExchange.getRequestHeaders().getFirst("webhook-timestamp"), httpExchange.getRequestHeaders().getFirst("webhook-signature"), bodyBytes));
                Integer failureStatus = this.failureStatusesByEventId.get(eventId);
                if (Objects.isNull(failureStatus)) {
                    httpExchange.sendResponseHeaders(204, -1L);
                } else {
                    httpExchange.sendResponseHeaders(failureStatus, bodyBytes.length);
                    httpExchange.getResponseBody().write(bodyBytes);
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
