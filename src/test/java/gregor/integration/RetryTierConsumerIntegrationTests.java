package gregor.integration;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import gregor.avro.WebhookEvent;
import gregor.route.WebhookRoute;
import gregor.service.WebhookService;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
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
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.Comparator;
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

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Import(RetryTierConsumerIntegrationTests.FixedClockConfiguration.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class RetryTierConsumerIntegrationTests {

    private static final Instant ELIGIBILITY_INSTANT = Instant.parse("2026-07-25T12:00:00Z");
    private static final String TENANT_ID = "00000000-0000-0000-0000-000000000001";
    private static final String ENDPOINT_ID = "00000000-0000-0000-0000-000000000002";
    private static final String SIGNING_SECRET = "whsec_" + Base64.getEncoder().encodeToString(
            "01234567890123456789012345678901".getBytes(StandardCharsets.UTF_8)
    );
    private static final List<String> RETRY_TOPICS = List.of(
            WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC,
            WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC,
            WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC
    );
    private static final TestReceiver RECEIVER = TestReceiver.start();

    @Autowired
    private CamelContext camelContext;
    @Autowired
    private ProducerTemplate producerTemplate;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @MockitoSpyBean
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
        registry.add("camel.component.kafka.consumers-count", () -> "2");
        registry.add("camel.component.kafka.auto-commit-enable", () -> "false");
        registry.add("camel.component.kafka.allow-manual-commit", () -> "true");
        registry.add("camel.component.kafka.break-on-first-error", () -> "true");
        registry.add("camel.component.kafka.auto-offset-reset", () -> "earliest");
        registry.add("camel.component.kafka.max-poll-records", () -> "1");
        registry.add("camel.component.kafka.max-poll-interval-ms", () -> "4500000");
        registry.add("camel.component.kafka.additional-properties[session.timeout.ms]", () -> "6000");
        registry.add("camel.component.kafka.additional-properties[heartbeat.interval.ms]", () -> "1000");
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
        registry.add("gregor.kafka.listener-concurrency", () -> "2");
        registry.add("gregor.webhook.tenant-id", () -> TENANT_ID);
        registry.add("gregor.webhook.endpoint-id", () -> ENDPOINT_ID);
        registry.add("gregor.webhook.receiver-url", RECEIVER::uri);
        registry.add("gregor.webhook.signing-secret", () -> SIGNING_SECRET);
        registry.add("gregor.webhook.connection-timeout", () -> "2s");
        registry.add("gregor.webhook.response-timeout", () -> "5s");
        registry.add("gregor.webhook.receiver-timestamp-tolerance", () -> "5m");
    }

    @BeforeEach
    void resetState() {
        RECEIVER.reset();
    }

    @AfterAll
    static void stopReceiver() {
        RECEIVER.close();
    }

    @Test
    void registersOneSeparateActiveConsumerGroupForEveryRetryTier() throws Exception {
        try (AdminClient adminClient = this.adminClient()) {
            for (String retryTopic : RETRY_TOPICS) {
                ConsumerGroupDescription description = this.awaitActiveConsumerGroup(adminClient, retryTopic);
                Assertions.assertThat(description.members()).allSatisfy(member ->
                        Assertions.assertThat(member.assignment().topicPartitions())
                                .extracting(TopicPartition::topic)
                                .containsOnly(retryTopic)
                );
            }
        }
    }

    @Test
    void acceptsARetryRecordExactlyAtTheEligibilityInstantWithoutAPositiveWait() throws Exception {
        WebhookEvent event = event("exact-eligibility.test", 0, 7, 3, ELIGIBILITY_INSTANT);
        this.send(WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC, event);
        Map<String, Object> row = this.awaitAttemptRow(event.getEventId());
        Assertions.assertThat(row)
                .containsEntry("attempt_number", 1)
                .containsEntry("outcome", "SUCCEEDED")
                .containsEntry("retry_count", 7)
                .containsEntry("replay_count", 3)
                .containsEntry("dead_lettered", false);
    }

    @Test
    void deliversAnOverdueBacklogRecordWhenItsRetryRouteStarts() throws Exception {
        String topic = WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC;
        String routeId = retryConsumerUri(topic);
        WebhookEvent event = event("overdue-startup.test", 2, 2, 1, ELIGIBILITY_INSTANT.minusSeconds(600));
        boolean routeRestarted = false;

        this.camelContext.getRouteController().stopRoute(routeId);
        try {
            this.send(topic, event);
            Assertions.assertThat(RECEIVER.count(event.getEventId())).isZero();
            Assertions.assertThat(this.attemptRowCount(event.getEventId())).isZero();

            this.camelContext.getRouteController().startRoute(routeId);
            routeRestarted = true;
            RECEIVER.awaitRequest(event.getEventId(), 1);
            Assertions.assertThat(this.awaitAttemptRow(event.getEventId()))
                    .containsEntry("attempt_number", 3)
                    .containsEntry("outcome", "SUCCEEDED")
                    .containsEntry("retry_count", 2)
                    .containsEntry("replay_count", 1);
        } finally {
            if (!routeRestarted) {
                this.camelContext.getRouteController().startRoute(routeId);
            }
        }
    }

    @Test
    void eligibilityWaitBlocksOnlyThePartitionsAssignedToTheWaitingConsumer() throws Exception {
        String topic = WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC;
        RetryPartitionSelection partitions = this.retryPartitionSelection(topic);
        WebhookEvent waiting = event("waiting-tier.test", 0, 1, 0, ELIGIBILITY_INSTANT.plusSeconds(1));
        WebhookEvent sameConsumer = event("same-consumer.test", 0, 2, 0, ELIGIBILITY_INSTANT);
        WebhookEvent otherConsumer = event("other-consumer.test", 1, 2, 0, ELIGIBILITY_INSTANT);
        WebhookEvent otherTier = event("other-tier.test", 1, 1, 0, ELIGIBILITY_INSTANT);
        WebhookEvent source = event("source-progress.test", 0, 0, 0, ELIGIBILITY_INSTANT.plusSeconds(3_600));
        CountDownLatch waitingStarted = new CountDownLatch(1);
        CountDownLatch releaseWait = new CountDownLatch(1);
        Mockito.doAnswer(_ -> {
            waitingStarted.countDown();
            if (!releaseWait.await(30, TimeUnit.SECONDS)) {
                throw new AssertionError("Eligibility wait was not released");
            }
            return null;
        }).when(this.webhookService).awaitEligibility(Mockito.argThat(
                message -> waiting.getEventId().equals(message.webhookEvent().getEventId())
        ));

        try {
            this.send(topic, waiting, partitions.waitingPartition());
            Assertions.assertThat(waitingStarted.await(15, TimeUnit.SECONDS)).isTrue();
            this.send(topic, sameConsumer, partitions.sameConsumerPartition());
            this.send(topic, otherConsumer, partitions.otherConsumerPartition());
            this.send(WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC, otherTier);
            this.send(WebhookRoute.WEBHOOK_EVENT_TOPIC, source);

            for (WebhookEvent independent : List.of(otherConsumer, otherTier, source)) {
                RECEIVER.awaitRequest(independent.getEventId(), 1);
            }
            Assertions.assertThat(RECEIVER.count(waiting.getEventId())).isZero();
            Assertions.assertThat(RECEIVER.count(sameConsumer.getEventId())).isZero();
        } finally {
            releaseWait.countDown();
        }

        for (WebhookEvent event : List.of(waiting, sameConsumer, otherConsumer, otherTier, source)) {
            this.awaitAttemptRows(event.getEventId(), 1);
        }
    }

    @Test
    void keepsHeartbeatingWhileEligibilityWaitExceedsTheSessionTimeout() throws Exception {
        String topic = WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC;
        RetryPartitionSelection partitions = this.retryPartitionSelection(topic);
        TopicPartition waitingPartition = new TopicPartition(topic, partitions.waitingPartition());
        String initialConsumerId;
        try (AdminClient adminClient = this.adminClient()) {
            ConsumerGroupDescription description = this.awaitActiveConsumerGroup(adminClient, topic);
            initialConsumerId = ownerId(description, waitingPartition);
        }
        WebhookEvent waiting = event(
                "heartbeat-wait.test",
                0,
                1,
                0,
                ELIGIBILITY_INSTANT.plusSeconds(10)
        );

        this.send(topic, waiting, waitingPartition.partition());
        Mockito.verify(this.webhookService, Mockito.timeout(5_000)).awaitEligibility(Mockito.argThat(message ->
                waiting.getEventId().equals(message.webhookEvent().getEventId())
        ));
        Thread.sleep(7_000L);

        Assertions.assertThat(RECEIVER.count(waiting.getEventId())).isZero();
        try (AdminClient adminClient = this.adminClient()) {
            ConsumerGroupDescription description = this.awaitActiveConsumerGroup(adminClient, topic);
            Assertions.assertThat(ownerId(description, waitingPartition)).isEqualTo(initialConsumerId);
        }

        RECEIVER.awaitRequest(waiting.getEventId(), 1);
        Assertions.assertThat(this.awaitAttemptRow(waiting.getEventId())).containsEntry("attempt_number", 1);
    }

    @Test
    void keepsCumulativeNumberingAndRecursiveRetriesForAnEligibleRetryRecord() throws Exception {
        RECEIVER.respondWith(503);
        WebhookEvent event = event("cumulative-retry.test", 1, 7, 3, ELIGIBILITY_INSTANT);

        this.send(WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC, event);

        RECEIVER.awaitRequest(event.getEventId(), 3);
        this.awaitAttemptRows(event.getEventId(), 3);
        Assertions.assertThat(RECEIVER.count(event.getEventId())).isEqualTo(3);
        List<Map<String, Object>> rows = this.jdbcTemplate.queryForList(
                """
                SELECT attempt_number, outcome, http_status, retry_count, replay_count, dead_lettered
                FROM delivery_attempt
                WHERE event_id = ?::uuid
                ORDER BY attempt_number
                """,
                event.getEventId()
        );
        Assertions.assertThat(rows)
                .extracting(row -> (Integer) row.get("attempt_number"))
                .containsExactly(2, 3, 4);
        Assertions.assertThat(rows).allSatisfy(row -> Assertions.assertThat(row)
                .containsEntry("outcome", "RETRYABLE_FAILURE")
                .containsEntry("http_status", 503)
                .containsEntry("retry_count", 7)
                .containsEntry("replay_count", 3)
                .containsEntry("dead_lettered", false));
    }

    private static String ownerId(
            ConsumerGroupDescription description,
            TopicPartition partition
    ) {
        return description.members().stream()
                .filter(member -> member.assignment().topicPartitions().contains(partition))
                .map(MemberDescription::consumerId)
                .findFirst()
                .orElseThrow(() -> new AssertionError("No consumer owns " + partition));
    }

    private RetryPartitionSelection retryPartitionSelection(String topic) throws Exception {
        try (AdminClient adminClient = this.adminClient()) {
            ConsumerGroupDescription description = this.awaitActiveConsumerGroup(adminClient, topic);
            List<List<TopicPartition>> memberAssignments = description.members().stream()
                    .map(member -> member.assignment().topicPartitions().stream()
                            .filter(partition -> topic.equals(partition.topic()))
                            .sorted(Comparator.comparingInt(TopicPartition::partition))
                            .toList())
                    .filter(partitions -> !partitions.isEmpty())
                    .toList();
            Assertions.assertThat(memberAssignments).hasSize(2);
            Assertions.assertThat(memberAssignments.getFirst()).hasSizeGreaterThanOrEqualTo(2);
            return new RetryPartitionSelection(
                    memberAssignments.getFirst().getFirst().partition(),
                    memberAssignments.getFirst().get(1).partition(),
                    memberAssignments.get(1).getFirst().partition()
            );
        }
    }

    private ConsumerGroupDescription awaitActiveConsumerGroup(
            AdminClient adminClient,
            String groupId
    ) throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(15);
        while (Instant.now().isBefore(deadline)) {
            try {
                ConsumerGroupDescription description = adminClient
                        .describeConsumerGroups(List.of(groupId))
                        .all()
                        .get(2, TimeUnit.SECONDS)
                        .get(groupId);
                if (description.groupState() == GroupState.STABLE && description.members().size() == 2) {
                    return description;
                }
            } catch (ExecutionException | TimeoutException ignored) {
                // The group can be absent or rebalancing while the application consumer starts.
            }
            Thread.sleep(50L);
        }
        throw new AssertionError("Expected active consumer group " + groupId);
    }

    private void send(String topic, WebhookEvent event) {
        this.send(topic, event, 0);
    }

    private void send(String topic, WebhookEvent event, int partition) {
        this.producerTemplate.sendBodyAndHeaders(
                "kafka:" + topic,
                event,
                Map.of(KafkaConstants.KEY, event.getEndpointId(), KafkaConstants.PARTITION_KEY, partition)
        );
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
        Integer count = this.jdbcTemplate.queryForObject(
                "SELECT count(*) FROM delivery_attempt WHERE event_id = ?::uuid",
                Integer.class,
                eventId
        );
        return count == null ? 0 : count;
    }

    private AdminClient adminClient() {
        return AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:" + System.getenv().getOrDefault("KAFKA_PORT", "9092")
        ));
    }

    private static String retryConsumerUri(String topic) {
        return "kafka:" + topic + "?groupId=" + topic;
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

    private record RetryPartitionSelection(
            int waitingPartition,
            int sameConsumerPartition,
            int otherConsumerPartition
    ) {
    }

    private record CapturedRequest(String webhookId) {
    }

    private static final class TestReceiver implements AutoCloseable {

        private final HttpServer server;
        private final ExecutorService executor;
        private final ConcurrentLinkedQueue<CapturedRequest> requests = new ConcurrentLinkedQueue<>();
        private volatile int status = 204;

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
                throw new IllegalStateException("Could not start retry-tier receiver", exception);
            }
        }

        String uri() {
            return "http://localhost:" + this.server.getAddress().getPort() + "/webhook";
        }

        void respondWith(int responseStatus) {
            this.status = responseStatus;
        }

        void reset() {
            this.requests.clear();
            this.status = 204;
        }

        int count(String eventId) {
            return (int) this.requests.stream()
                    .filter(request -> eventId.equals(request.webhookId()))
                    .count();
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

        private void handle(HttpExchange exchange) throws IOException {
            exchange.getRequestBody().readAllBytes();
            this.requests.add(new CapturedRequest(exchange.getRequestHeaders().getFirst("webhook-id")));
            try (exchange) {
                exchange.sendResponseHeaders(this.status, -1L);
            }
        }

        @Override
        public void close() {
            this.server.stop(0);
            this.executor.shutdownNow();
        }
    }
}
