package gregor.integration;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import gregor.avro.DeliveryAttempt;
import gregor.avro.WebhookEvent;
import gregor.route.WebhookRoute;
import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.vertx.core.Vertx;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = "gregor.webhook.maximum-attempts=4")
class ImpossibleDeliveryStateIntegrationTests {

    private static final int MAXIMUM_ATTEMPTS = 4;
    private static final String TENANT_ID = "00000000-0000-0000-0000-000000000001";
    private static final String ENDPOINT_ID = "00000000-0000-0000-0000-000000000002";
    private static final String PAYLOAD_MARKER = "impossible-state-payload-marker";
    private static final String SIGNING_SECRET = "whsec_" + Base64.getEncoder().encodeToString(
            "01234567890123456789012345678901".getBytes(StandardCharsets.UTF_8)
    );
    private static final List<String> NON_PRODUCED_TOPICS = List.of(
            WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC,
            WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC,
            WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC,
            WebhookRoute.WEBHOOK_EVENT_DEAD_LETTER_TOPIC
    );
    private static final TestReceiver RECEIVER = TestReceiver.start();

    @Autowired
    private ProducerTemplate producerTemplate;
    @Autowired
    private Vertx registryVertx;
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
        List<String> discardedEventIds = impossibleEvents.stream().map(WebhookEvent::getEventId).toList();
        TopicPartition partition = new TopicPartition(WebhookRoute.WEBHOOK_EVENT_TOPIC, 0);
        try (
                KafkaConsumer<String, Object> downstreamConsumer =
                        this.newConsumer("integration-impossible-state-" + UUID.randomUUID())
        ) {
            this.seekToEnd(downstreamConsumer, downstreamTopics());

            for (WebhookEvent impossibleEvent : impossibleEvents) {
                this.producerTemplate.sendBodyAndHeaders(
                        "kafka:" + WebhookRoute.WEBHOOK_EVENT_TOPIC,
                        impossibleEvent,
                        Map.of(KafkaConstants.KEY, ENDPOINT_ID, KafkaConstants.PARTITION_KEY, 0)
                );

                this.awaitCommittedOffset(WebhookRoute.WEBHOOK_EVENT_TOPIC, partition, this.endOffset(partition));
                Assertions.assertThat(RECEIVER.count(impossibleEvent.getEventId())).isZero();
                Assertions.assertThat(this.attemptRowCount(impossibleEvent.getEventId())).isZero();
            }

            List<ConsumerRecord<String, Object>> downstreamRecords = new ArrayList<>();
            downstreamConsumer.poll(Duration.ofSeconds(1)).forEach(downstreamRecords::add);
            Assertions.assertThat(downstreamRecords)
                    .extracting(record -> eventId(record.value()))
                    .doesNotContainAnyElementsOf(discardedEventIds);
            Assertions.assertThat(downstreamRecords)
                    .extracting(ConsumerRecord::topic)
                    .doesNotContainAnyElementsOf(NON_PRODUCED_TOPICS);
        }
    }

    @Test
    void discardsMalformedRetryEnvelopesWithoutDeliveryOrDownstreamPublicationAndCommitsOffsets()
            throws Exception {
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
        List<String> discardedEventIds = malformedRecords.stream()
                .map(record -> record.event().getEventId())
                .toList();
        Map<String, Long> startingRetryOffsets = new HashMap<>();
        Map<String, Integer> sentRecordsByTopic = new HashMap<>();
        for (String retryTopic : NON_PRODUCED_TOPICS.subList(0, 3)) {
            startingRetryOffsets.put(retryTopic, this.endOffset(new TopicPartition(retryTopic, 0)));
        }

        try (
                KafkaConsumer<String, Object> downstreamConsumer =
                        this.newConsumer("integration-malformed-retry-" + UUID.randomUUID())
        ) {
            this.seekToEnd(
                    downstreamConsumer,
                    WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC,
                    WebhookRoute.WEBHOOK_EVENT_DEAD_LETTER_TOPIC
            );

            for (RetryRecord malformedRecord : malformedRecords) {
                TopicPartition partition = new TopicPartition(malformedRecord.topic(), 0);
                this.producerTemplate.sendBodyAndHeaders(
                        "kafka:" + malformedRecord.topic(),
                        malformedRecord.event(),
                        Map.of(KafkaConstants.KEY, ENDPOINT_ID, KafkaConstants.PARTITION_KEY, 0)
                );
                sentRecordsByTopic.merge(malformedRecord.topic(), 1, Integer::sum);

                this.awaitCommittedOffset(malformedRecord.topic(), partition, this.endOffset(partition));
                Assertions.assertThat(RECEIVER.count(malformedRecord.event().getEventId())).isZero();
                Assertions.assertThat(this.attemptRowCount(malformedRecord.event().getEventId())).isZero();
            }

            List<ConsumerRecord<String, Object>> downstreamRecords = new ArrayList<>();
            downstreamConsumer.poll(Duration.ofSeconds(1)).forEach(downstreamRecords::add);
            Assertions.assertThat(downstreamRecords)
                    .extracting(record -> eventId(record.value()))
                    .doesNotContainAnyElementsOf(discardedEventIds);
        }

        for (Map.Entry<String, Long> startingOffset : startingRetryOffsets.entrySet()) {
            long expectedOffset = startingOffset.getValue() + sentRecordsByTopic.get(startingOffset.getKey());
            Assertions.assertThat(this.endOffset(new TopicPartition(startingOffset.getKey(), 0)))
                    .isEqualTo(expectedOffset);
        }
    }

    @Test
    void deliversAnEnvelopeWithAFutureNextEligibleAtWithoutDelayingOrSkippingIt() throws Exception {
        WebhookEvent eligibleLaterEvent = event(
                "next-eligible-at.test",
                0,
                0,
                0,
                Instant.now().plus(Duration.ofHours(1))
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
        properties.put(SchemaResolverConfig.VERTX_INSTANCE, this.registryVertx);
        return new KafkaConsumer<>(properties);
    }

    private void seekToEnd(KafkaConsumer<String, Object> consumer, String... topics) {
        List<TopicPartition> partitions = Stream.of(topics)
                .flatMap(topic -> consumer.partitionsFor(topic, Duration.ofSeconds(10)).stream()
                        .map(PartitionInfo::partition)
                        .map(partition -> new TopicPartition(topic, partition)))
                .toList();
        consumer.assign(partitions);
        consumer.seekToEnd(partitions);
        for (TopicPartition partition : partitions) {
            consumer.position(partition);
        }
    }

    private long endOffset(TopicPartition partition) throws Exception {
        try (AdminClient adminClient = this.adminClient()) {
            return adminClient.listOffsets(Map.of(partition, OffsetSpec.latest()))
                    .partitionResult(partition)
                    .get(5, TimeUnit.SECONDS)
                    .offset();
        }
    }

    private void awaitCommittedOffset(
            String groupId,
            TopicPartition partition,
            long expectedOffset
    ) throws Exception {
        try (AdminClient adminClient = this.adminClient()) {
            Instant deadline = Instant.now().plusSeconds(15);
            while (Instant.now().isBefore(deadline)) {
                Map<TopicPartition, OffsetAndMetadata> offsets = adminClient
                        .listConsumerGroupOffsets(groupId)
                        .partitionsToOffsetAndMetadata()
                        .get(5, TimeUnit.SECONDS);
                OffsetAndMetadata committed = offsets.get(partition);
                if (committed != null && committed.offset() >= expectedOffset) {
                    return;
                }
                Thread.sleep(50L);
            }
        }
        Assertions.fail("Offset was not committed after the record was discarded from group " + groupId);
    }

    private AdminClient adminClient() {
        return AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers()));
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

    private static String[] downstreamTopics() {
        return Stream.concat(
                Stream.of(WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC),
                NON_PRODUCED_TOPICS.stream()
        ).toArray(String[]::new);
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

    private static String eventId(Object value) {
        if (value instanceof WebhookEvent event) {
            return event.getEventId();
        }
        if (value instanceof DeliveryAttempt attempt) {
            return attempt.getEventId();
        }
        return null;
    }

    private static String kafkaBootstrapServers() {
        return "localhost:" + System.getenv().getOrDefault("KAFKA_PORT", "9092");
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
