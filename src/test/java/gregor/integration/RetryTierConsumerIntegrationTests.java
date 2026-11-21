package gregor.integration;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import gregor.avro.WebhookEvent;
import gregor.route.WebhookRoute;
import gregor.service.WebhookService;
import io.vertx.core.Vertx;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Import(RetryTierConsumerIntegrationTests.FixedClockConfiguration.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = "gregor.webhook.maximum-attempts=4")
class RetryTierConsumerIntegrationTests {

    private static final Instant ELIGIBILITY_INSTANT = Instant.parse("2026-07-25T12:00:00Z");
    private static final Duration SESSION_TIMEOUT = Duration.ofSeconds(6);
    private static final String TENANT_ID = "00000000-0000-0000-0000-000000000001";
    private static final String ENDPOINT_ID = "00000000-0000-0000-0000-000000000002";
    private static final String SIGNING_SECRET = "whsec_" + Base64.getEncoder().encodeToString("01234567890123456789012345678901".getBytes(StandardCharsets.UTF_8));
    private static final List<String> RETRY_TOPICS = List.of(WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC, WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC, WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC);
    private static final TestReceiver TEST_RECEIVER = TestReceiver.start();

    @Autowired
    private CamelContext camelContext;
    @Autowired
    private ProducerTemplate producerTemplate;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private Vertx registryVertx;
    @MockitoSpyBean
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
        dynamicPropertyRegistry.add("camel.component.kafka.consumers-count", () -> "2");
        dynamicPropertyRegistry.add("camel.component.kafka.auto-commit-enable", () -> "false");
        dynamicPropertyRegistry.add("camel.component.kafka.allow-manual-commit", () -> "true");
        dynamicPropertyRegistry.add("camel.component.kafka.break-on-first-error", () -> "true");
        dynamicPropertyRegistry.add("camel.component.kafka.auto-offset-reset", () -> "earliest");
        dynamicPropertyRegistry.add("camel.component.kafka.max-poll-records", () -> "1");
        dynamicPropertyRegistry.add("camel.component.kafka.max-poll-interval-ms", () -> "4500000");
        dynamicPropertyRegistry.add("camel.component.kafka.additional-properties[session.timeout.ms]", () -> Long.toString(SESSION_TIMEOUT.toMillis()));
        dynamicPropertyRegistry.add("camel.component.kafka.additional-properties[heartbeat.interval.ms]", () -> "1000");
        dynamicPropertyRegistry.add("camel.main.auto-startup", () -> "true");
        dynamicPropertyRegistry.add("camel.component.kafka.request-required-acks", () -> "all");
        dynamicPropertyRegistry.add("camel.component.kafka.key-serializer", () -> "org.apache.kafka.common.serialization.StringSerializer");
        dynamicPropertyRegistry.add("camel.component.kafka.value-serializer", () -> "io.apicurio.registry.serde.avro.AvroKafkaSerializer");
        dynamicPropertyRegistry.add("camel.component.kafka.key-deserializer", () -> "org.apache.kafka.common.serialization.StringDeserializer");
        dynamicPropertyRegistry.add("camel.component.kafka.value-deserializer", () -> "io.apicurio.registry.serde.avro.AvroKafkaDeserializer");
        dynamicPropertyRegistry.add("camel.component.kafka.additional-properties[apicurio.registry.url]", () -> "http://localhost:" + registryPort + "/apis/registry/v3");
        dynamicPropertyRegistry.add("camel.component.kafka.additional-properties[apicurio.registry.auto-register]", () -> "true");
        dynamicPropertyRegistry.add("camel.component.kafka.additional-properties[apicurio.registry.use-specific-avro-reader]", () -> "true");
        dynamicPropertyRegistry.add("gregor.kafka.listener-concurrency", () -> "2");
        dynamicPropertyRegistry.add("gregor.webhook.tenant-id", () -> TENANT_ID);
        dynamicPropertyRegistry.add("gregor.webhook.endpoint-id", () -> ENDPOINT_ID);
        dynamicPropertyRegistry.add("gregor.webhook.receiver-url", TEST_RECEIVER::uri);
        dynamicPropertyRegistry.add("gregor.webhook.signing-secret", () -> SIGNING_SECRET);
        dynamicPropertyRegistry.add("gregor.webhook.connection-timeout", () -> "2s");
        dynamicPropertyRegistry.add("gregor.webhook.response-timeout", () -> "5s");
        dynamicPropertyRegistry.add("gregor.webhook.receiver-timestamp-tolerance", () -> "5m");
    }

    @BeforeEach
    void resetState() {
        TEST_RECEIVER.reset();
    }

    @AfterAll
    static void stopReceiver() {
        TEST_RECEIVER.close();
    }

    @Test
    void registersOneSeparateActiveConsumerGroupForEveryRetryTier() throws Exception {
        try (AdminClient adminClient = KafkaTestRecords.adminClient()) {
            for (String retryTopic : RETRY_TOPICS) {
                ConsumerGroupDescription consumerGroupDescription = this.awaitActiveConsumerGroup(adminClient, retryTopic);
                Assertions.assertThat(consumerGroupDescription.members()).allSatisfy(memberDescription -> Assertions.assertThat(memberDescription.assignment().topicPartitions()).extracting(TopicPartition::topic).containsOnly(retryTopic));
            }
        }
    }

    @Test
    void acceptsARetryRecordExactlyAtTheEligibilityInstantWithoutAPositiveWait() throws Exception {
        WebhookEvent webhookEvent = event("exact-eligibility.test", 0, 7, 3, ELIGIBILITY_INSTANT);
        this.send(WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC, webhookEvent);
        Map<String, Object> attemptColumns = this.awaitAttemptRow(webhookEvent.getEventId());
        Assertions.assertThat(attemptColumns).containsEntry("attempt_number", 1).containsEntry("outcome", "SUCCEEDED").containsEntry("retry_count", 7).containsEntry("replay_count", 3).containsEntry("dead_lettered", false);
    }

    @Test
    void deliversAnOverdueBacklogRecordWhenItsRetryRouteStarts() throws Exception {
        String topic = WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC;
        String routeId = retryConsumerUri(topic);
        WebhookEvent webhookEvent = event("overdue-startup.test", 2, 2, 1, ELIGIBILITY_INSTANT.minusSeconds(600));
        boolean routeRestarted = false;

        this.camelContext.getRouteController().stopRoute(routeId);
        try {
            this.send(topic, webhookEvent);
            Assertions.assertThat(TEST_RECEIVER.count(webhookEvent.getEventId())).isZero();
            Assertions.assertThat(this.attemptRowCount(webhookEvent.getEventId())).isZero();

            this.camelContext.getRouteController().startRoute(routeId);
            routeRestarted = true;
            TEST_RECEIVER.awaitRequest(webhookEvent.getEventId(), 1);
            Assertions.assertThat(this.awaitAttemptRow(webhookEvent.getEventId())).containsEntry("attempt_number", 3).containsEntry("outcome", "SUCCEEDED").containsEntry("retry_count", 2).containsEntry("replay_count", 1);
        } finally {
            if (!routeRestarted) {
                this.camelContext.getRouteController().startRoute(routeId);
            }
        }
    }

    @Test
    void eligibilityWaitBlocksOnlyThePartitionsAssignedToTheWaitingConsumer() throws Exception {
        String topic = WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC;
        RetryPartitionSelection retryPartitionSelection = this.retryPartitionSelection(topic);
        WebhookEvent waitingWebhookEvent = event("waiting-tier.test", 0, 1, 0, ELIGIBILITY_INSTANT.plusSeconds(1));
        WebhookEvent sameConsumerWebhookEvent = event("same-consumer.test", 0, 2, 0, ELIGIBILITY_INSTANT);
        WebhookEvent otherConsumerWebhookEvent = event("other-consumer.test", 1, 2, 0, ELIGIBILITY_INSTANT);
        WebhookEvent otherTierWebhookEvent = event("other-tier.test", 1, 1, 0, ELIGIBILITY_INSTANT);
        WebhookEvent sourceWebhookEvent = event("source-progress.test", 0, 0, 0, ELIGIBILITY_INSTANT.plusSeconds(3_600));
        CountDownLatch waitingStartedCountDownLatch = new CountDownLatch(1);
        CountDownLatch releaseWaitCountDownLatch = new CountDownLatch(1);
        Mockito.doAnswer(_ -> {
                    waitingStartedCountDownLatch.countDown();
                    if (!releaseWaitCountDownLatch.await(30, TimeUnit.SECONDS)) {
                        throw new AssertionError("Eligibility wait was not released");
                    }
                    return null;
                })
                .when(this.webhookService)
                .awaitEligibility(Mockito.argThat(webhookRouteMessage -> Objects.equals(waitingWebhookEvent.getEventId(), webhookRouteMessage.webhookEvent().getEventId())));

        try {
            this.send(topic, waitingWebhookEvent, retryPartitionSelection.waitingPartition());
            Assertions.assertThat(waitingStartedCountDownLatch.await(15, TimeUnit.SECONDS)).isTrue();
            this.send(topic, sameConsumerWebhookEvent, retryPartitionSelection.sameConsumerPartition());
            this.send(topic, otherConsumerWebhookEvent, retryPartitionSelection.otherConsumerPartition());
            this.send(WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC, otherTierWebhookEvent);
            this.send(WebhookRoute.WEBHOOK_EVENT_TOPIC, sourceWebhookEvent);

            for (WebhookEvent independentWebhookEvent : List.of(otherConsumerWebhookEvent, otherTierWebhookEvent, sourceWebhookEvent)) {
                TEST_RECEIVER.awaitRequest(independentWebhookEvent.getEventId(), 1);
            }
            Assertions.assertThat(TEST_RECEIVER.count(waitingWebhookEvent.getEventId())).isZero();
            Assertions.assertThat(TEST_RECEIVER.count(sameConsumerWebhookEvent.getEventId())).isZero();
        } finally {
            releaseWaitCountDownLatch.countDown();
        }

        for (WebhookEvent webhookEvent : List.of(waitingWebhookEvent, sameConsumerWebhookEvent, otherConsumerWebhookEvent, otherTierWebhookEvent, sourceWebhookEvent)) {
            this.awaitAttemptRows(webhookEvent.getEventId(), 1);
        }
    }

    @Test
    void keepsHeartbeatingWhileEligibilityWaitExceedsTheSessionTimeout() throws Exception {
        String topic = WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC;
        RetryPartitionSelection retryPartitionSelection = this.retryPartitionSelection(topic);
        TopicPartition waitingTopicPartition = new TopicPartition(topic, retryPartitionSelection.waitingPartition());
        String initialConsumerId;
        try (AdminClient adminClient = KafkaTestRecords.adminClient()) {
            ConsumerGroupDescription consumerGroupDescription = this.awaitActiveConsumerGroup(adminClient, topic);
            initialConsumerId = ownerId(consumerGroupDescription, waitingTopicPartition);
        }
        WebhookEvent waitingWebhookEvent = event("heartbeat-wait.test", 0, 1, 0, ELIGIBILITY_INSTANT.plusSeconds(10));

        CompletableFuture<Duration> eligibilityWaitFuture = new CompletableFuture<>();
        Mockito.doAnswer(invocationOnMock -> {
                    long waitStartedAtNanos = System.nanoTime();
                    invocationOnMock.callRealMethod();
                    eligibilityWaitFuture.complete(Duration.ofNanos(System.nanoTime() - waitStartedAtNanos));
                    return null;
                })
                .when(this.webhookService)
                .awaitEligibility(Mockito.argThat(webhookRouteMessage -> Objects.equals(waitingWebhookEvent.getEventId(), webhookRouteMessage.webhookEvent().getEventId())));

        this.send(topic, waitingWebhookEvent, waitingTopicPartition.partition());
        Assertions.assertThat(eligibilityWaitFuture.get(20, TimeUnit.SECONDS)).isGreaterThan(SESSION_TIMEOUT);
        TEST_RECEIVER.awaitRequest(waitingWebhookEvent.getEventId(), 1);
        try (AdminClient adminClient = KafkaTestRecords.adminClient()) {
            ConsumerGroupDescription consumerGroupDescription = this.awaitActiveConsumerGroup(adminClient, topic);
            Assertions.assertThat(ownerId(consumerGroupDescription, waitingTopicPartition)).isEqualTo(initialConsumerId);
        }

        Assertions.assertThat(this.awaitAttemptRow(waitingWebhookEvent.getEventId())).containsEntry("attempt_number", 1);
    }

    @Test
    void deadLettersBelowTheMaximumWhenNoHigherTierExists() throws Exception {
        WebhookEvent webhookEvent = event("cumulative-retry.test", 1, 7, 3, ELIGIBILITY_INSTANT);
        WebhookEvent unrelatedWebhookEvent = event("unrelated-retry.test", 1, 7, 3, ELIGIBILITY_INSTANT);
        TEST_RECEIVER.failOnly(webhookEvent.getEventId());
        String inputTopic = WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC;
        try (AdminClient adminClient = KafkaTestRecords.adminClient()) {
            Map<TopicPartition, Long> expectedRetryOffsets = new HashMap<>(KafkaTestRecords.retryEndOffsets(adminClient));
            Map<TopicPartition, Long> expectedDeadLetterOffsets = new HashMap<>(KafkaTestRecords.endOffsets(adminClient, "webhook-events-dead-letter"));
            try (
                    KafkaConsumer<String, Object> inputKafkaConsumer = KafkaTestRecords.consumer(this.registryVertx, KafkaTestRecords.endOffsets(adminClient, inputTopic));
                    KafkaConsumer<String, Object> deadLetterKafkaConsumer = KafkaTestRecords.consumer(this.registryVertx, expectedDeadLetterOffsets)
            ) {
                for (WebhookEvent webhookEventToSend : List.of(unrelatedWebhookEvent, webhookEvent)) {
                    this.send(inputTopic, webhookEventToSend);
                    KafkaTestRecords.awaitCommit(adminClient, KafkaTestRecords.awaitRecord(inputKafkaConsumer, webhookEventToSend.getEventId()));
                }
                ConsumerRecord<String, Object> deadLetterConsumerRecord = KafkaTestRecords.awaitRecord(deadLetterKafkaConsumer, webhookEvent.getEventId());
                Assertions.assertThat(deadLetterConsumerRecord.key()).isEqualTo(webhookEvent.getEndpointId());
                Assertions.assertThat(deadLetterConsumerRecord.value()).isEqualTo(WebhookEvent.newBuilder(webhookEvent).setDeliveryAttemptCount(2).build());
                expectedRetryOffsets.compute(new TopicPartition(inputTopic, 0), (_, offset) -> offset + 2);
                expectedDeadLetterOffsets.compute(new TopicPartition("webhook-events-dead-letter", deadLetterConsumerRecord.partition()), (_, offset) -> offset + 1);
                Assertions.assertThat(KafkaTestRecords.retryEndOffsets(adminClient)).isEqualTo(expectedRetryOffsets);
                Assertions.assertThat(KafkaTestRecords.endOffsets(adminClient, "webhook-events-dead-letter")).isEqualTo(expectedDeadLetterOffsets);
            }
        } finally {
            TEST_RECEIVER.clearRetryableFailure();
        }
        Assertions.assertThat(this.awaitAttemptRow(unrelatedWebhookEvent.getEventId())).containsEntry("outcome", "SUCCEEDED").containsEntry("http_status", 204).containsEntry("dead_lettered", false);
        Assertions.assertThat(TEST_RECEIVER.count(webhookEvent.getEventId())).isEqualTo(1);
        Assertions.assertThat(this.awaitAttemptRow(webhookEvent.getEventId()))
                .containsEntry("attempt_number", 2)
                .containsEntry("outcome", "RETRYABLE_FAILURE")
                .containsEntry("http_status", 503)
                .containsEntry("retry_count", 7)
                .containsEntry("replay_count", 3)
                .containsEntry("dead_lettered", true);
    }

    private static String ownerId(ConsumerGroupDescription consumerGroupDescription, TopicPartition topicPartition) {
        return consumerGroupDescription
                .members()
                .stream()
                .filter(memberDescription -> memberDescription.assignment().topicPartitions().contains(topicPartition))
                .map(MemberDescription::consumerId)
                .findFirst()
                .orElseThrow(() -> new AssertionError("No consumer owns " + topicPartition));
    }

    private RetryPartitionSelection retryPartitionSelection(String topic) throws Exception {
        try (AdminClient adminClient = KafkaTestRecords.adminClient()) {
            ConsumerGroupDescription consumerGroupDescription = this.awaitActiveConsumerGroup(adminClient, topic);
            List<List<TopicPartition>> memberAssignments = consumerGroupDescription
                    .members()
                    .stream()
                    .map(memberDescription -> memberDescription.assignment()
                            .topicPartitions()
                            .stream()
                            .filter(topicPartition -> Objects.equals(topic, topicPartition.topic()))
                            .sorted(Comparator.comparingInt(TopicPartition::partition))
                            .toList())
                    .filter(partitions -> !partitions.isEmpty())
                    .toList();
            Assertions.assertThat(memberAssignments).hasSize(2);
            Assertions.assertThat(memberAssignments.getFirst()).hasSizeGreaterThanOrEqualTo(2);
            return new RetryPartitionSelection(memberAssignments.getFirst().getFirst().partition(), memberAssignments.getFirst().get(1).partition(), memberAssignments.get(1).getFirst().partition());
        }
    }

    private ConsumerGroupDescription awaitActiveConsumerGroup(AdminClient adminClient, String groupId) throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(15);
        while (Instant.now().isBefore(deadline)) {
            try {
                ConsumerGroupDescription consumerGroupDescription = adminClient.describeConsumerGroups(List.of(groupId)).all().get(2, TimeUnit.SECONDS).get(groupId);
                if (Objects.equals(consumerGroupDescription.groupState(), GroupState.STABLE) && consumerGroupDescription.members().size() == 2) {
                    return consumerGroupDescription;
                }
            } catch (ExecutionException | TimeoutException exception) {
                // The group can be absent or rebalancing while the application consumer starts.
            }
            Thread.sleep(50L);
        }
        throw new AssertionError("Expected active consumer group " + groupId);
    }

    private void send(String topic, WebhookEvent webhookEvent) {
        this.send(topic, webhookEvent, 0);
    }

    private void send(String topic, WebhookEvent webhookEvent, int partition) {
        this.producerTemplate.sendBodyAndHeaders("kafka:" + topic, webhookEvent, Map.of(KafkaConstants.KEY, webhookEvent.getEndpointId(), KafkaConstants.PARTITION_KEY, partition));
    }

    private Map<String, Object> awaitAttemptRow(String eventId) throws InterruptedException {
        this.awaitAttemptRows(eventId, 1);
        return this.jdbcTemplate.queryForMap(
                """
                SELECT attempt_number, outcome, http_status, retry_count, replay_count, dead_lettered
                FROM delivery_attempt
                WHERE event_id = ?::uuid
                """,
                eventId
        );
    }

    private void awaitAttemptRows(String eventId, int expectedCount) throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(20);
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

    private static String retryConsumerUri(String topic) {
        return "kafka:" + topic + "?groupId=" + topic;
    }

    private static WebhookEvent event(String eventType, int deliveryAttemptCount, int retryCount, int replayCount, Instant nextEligibleAt) {
        return new WebhookEvent(
                UUID.randomUUID().toString(),
                eventType,
                TENANT_ID,
                ENDPOINT_ID,
                ELIGIBILITY_INSTANT,
                ByteBuffer.wrap(("{\"eventType\":\"" + eventType + "\"}").getBytes(StandardCharsets.UTF_8)),
                retryCount,
                nextEligibleAt,
                deliveryAttemptCount,
                replayCount
        );
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class FixedClockConfiguration {

        @Bean
        @Primary
        Clock fixedClock() {
            return Clock.fixed(ELIGIBILITY_INSTANT, ZoneOffset.UTC);
        }
    }

    private record RetryPartitionSelection(int waitingPartition, int sameConsumerPartition, int otherConsumerPartition) {
    }

    private record CapturedRequest(String webhookId) {
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static final class TestReceiver implements AutoCloseable {

        private final HttpServer httpServer;
        private final ExecutorService executorService;
        private final ConcurrentLinkedQueue<CapturedRequest> capturedRequests = new ConcurrentLinkedQueue<>();
        private volatile String retryableEventId;

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
                throw new IllegalStateException("Could not start retry-tier receiver", exception);
            }
        }

        String uri() {
            return "http://localhost:" + this.httpServer.getAddress().getPort() + "/webhook";
        }

        void failOnly(String eventId) {
            this.retryableEventId = eventId;
        }

        void clearRetryableFailure() {
            this.retryableEventId = null;
        }

        void reset() {
            this.capturedRequests.clear();
            this.clearRetryableFailure();
        }

        int count(String eventId) {
            return (int) this.capturedRequests.stream().filter(capturedRequest -> Objects.equals(eventId, capturedRequest.webhookId())).count();
        }

        void awaitRequest(String eventId, int expectedCount) throws InterruptedException {
            Instant deadline = Instant.now().plusSeconds(30);
            while (Instant.now().isBefore(deadline)) {
                if (this.count(eventId) >= expectedCount) {
                    return;
                }
                Thread.sleep(25L);
            }
            throw new AssertionError("Expected " + expectedCount + " requests for event " + eventId);
        }

        private void handle(HttpExchange httpExchange) throws IOException {
            httpExchange.getRequestBody().readAllBytes();
            String eventId = httpExchange.getRequestHeaders().getFirst("webhook-id");
            this.capturedRequests.add(new CapturedRequest(eventId));
            try (httpExchange) {
                httpExchange.sendResponseHeaders(Objects.nonNull(eventId) && Objects.equals(eventId, this.retryableEventId) ? 503 : 204, -1L);
            }
        }

        @Override
        public void close() {
            this.httpServer.stop(0);
            this.executorService.shutdownNow();
        }
    }
}
