package gregor.integration;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import gregor.avro.WebhookEvent;
import gregor.route.WebhookRoute;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.apache.camel.test.spring.junit5.UseAdviceWith;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@UseAdviceWith
@CamelSpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class RetryEligibilityWaitInterruptionIntegrationTests {

    private static final Instant INITIAL_INSTANT = Instant.parse("2026-07-25T12:00:00Z");
    private static final String TENANT_ID = "00000000-0000-0000-0000-000000000001";
    private static final String ENDPOINT_ID = "00000000-0000-0000-0000-000000000002";
    private static final String SIGNING_SECRET = "whsec_" + Base64.getEncoder().encodeToString(
            "01234567890123456789012345678901".getBytes(StandardCharsets.UTF_8)
    );
    private static final String RETRY_CONSUMER_URI =
            "kafka:" + WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC
                    + "?groupId=" + WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC;
    private static final TestReceiver RECEIVER = TestReceiver.start();

    @Autowired
    private CamelContext camelContext;
    @Autowired
    private ProducerTemplate producerTemplate;
    @MockitoBean
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
        registry.add("camel.component.kafka.consumers-count", () -> "1");
        registry.add("camel.component.kafka.auto-commit-enable", () -> "false");
        registry.add("camel.component.kafka.allow-manual-commit", () -> "true");
        registry.add("camel.component.kafka.break-on-first-error", () -> "true");
        registry.add("camel.component.kafka.auto-offset-reset", () -> "earliest");
        registry.add("camel.component.kafka.max-poll-records", () -> "1");
        registry.add("camel.component.kafka.max-poll-interval-ms", () -> "4500000");
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
    void interruptsAWaitThenRedeliversAndCommitsOnlyAfterRecoveredDelivery() throws Exception {
        AtomicReference<Instant> currentInstant = new AtomicReference<>(INITIAL_INSTANT);
        Mockito.when(this.clock.instant()).thenAnswer(_ -> currentInstant.get());
        Mockito.when(this.clock.getZone()).thenReturn(ZoneOffset.UTC);
        AtomicInteger eligibilityEvaluations = new AtomicInteger();
        AdviceWith.adviceWith(
                this.camelContext,
                WebhookRoute.WEBHOOK_RETRY_ELIGIBILITY_DIRECT,
                advice -> advice.weaveAddFirst().process(_ -> eligibilityEvaluations.incrementAndGet())
        );
        this.camelContext.start();
        this.camelContext.getRouteController().startRoute(WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT);
        this.camelContext.getRouteController().startRoute(WebhookRoute.WEBHOOK_DELIVERY_ATTEMPT_DIRECT);
        this.camelContext.getRouteController().startRoute(WebhookRoute.WEBHOOK_RETRY_ELIGIBILITY_DIRECT);
        this.camelContext.getRouteController().startRoute(RETRY_CONSUMER_URI);
        RECEIVER.blockResponses();

        String eventId = UUID.randomUUID().toString();
        Instant nextEligibleAt = INITIAL_INSTANT.plusSeconds(4_200);
        WebhookEvent event = new WebhookEvent(
                eventId,
                "interrupted-eligibility.test",
                TENANT_ID,
                ENDPOINT_ID,
                INITIAL_INSTANT,
                ByteBuffer.wrap("{\"interrupted\":true}".getBytes(StandardCharsets.UTF_8)),
                1,
                nextEligibleAt,
                1,
                2
        );
        this.producerTemplate.sendBodyAndHeaders(
                "kafka:" + WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC,
                event,
                Map.of(KafkaConstants.KEY, ENDPOINT_ID, KafkaConstants.PARTITION_KEY, 0)
        );
        TopicPartition partition = new TopicPartition(WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC, 0);
        long expectedOffset = this.endOffset(partition);

        this.awaitEligibilityEvaluations(eligibilityEvaluations, 1);
        Assertions.assertThat(RECEIVER.count(eventId)).isZero();
        this.assertOffsetUncommitted(partition, expectedOffset);
        boolean stopped = this.camelContext.getRouteController()
                .stopRoute(RETRY_CONSUMER_URI, 1, TimeUnit.SECONDS, false);
        Assertions.assertThat(stopped).isTrue();
        Assertions.assertThat(this.camelContext.getRouteController().getRouteStatus(RETRY_CONSUMER_URI).isStopped())
                .isTrue();
        int evaluationsBeforeRestart = eligibilityEvaluations.get();

        currentInstant.set(nextEligibleAt);
        this.camelContext.getRouteController().startRoute(RETRY_CONSUMER_URI);
        this.awaitEligibilityEvaluations(eligibilityEvaluations, evaluationsBeforeRestart + 1);
        RECEIVER.awaitRequest(eventId);
        RECEIVER.awaitBlockedResponse();
        this.assertOffsetUncommitted(partition, expectedOffset);

        RECEIVER.releaseResponses();
        this.awaitCommittedOffset(partition, expectedOffset);
        Assertions.assertThat(RECEIVER.count(eventId)).isEqualTo(1);
    }

    private void awaitEligibilityEvaluations(AtomicInteger evaluations, int expectedCount) throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(15);
        while (Instant.now().isBefore(deadline)) {
            if (evaluations.get() >= expectedCount) {
                return;
            }
            Thread.sleep(25L);
        }
        Assertions.fail("Expected " + expectedCount + " retry eligibility evaluations");
    }

    private long endOffset(TopicPartition partition) throws Exception {
        try (AdminClient adminClient = this.adminClient()) {
            return adminClient.listOffsets(Map.of(partition, OffsetSpec.latest()))
                    .partitionResult(partition)
                    .get(5, TimeUnit.SECONDS)
                    .offset();
        }
    }

    private void assertOffsetUncommitted(TopicPartition partition, long expectedOffset) throws Exception {
        try (AdminClient adminClient = this.adminClient()) {
            Map<TopicPartition, OffsetAndMetadata> offsets = adminClient
                    .listConsumerGroupOffsets(WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC)
                    .partitionsToOffsetAndMetadata()
                    .get(5, TimeUnit.SECONDS);
            OffsetAndMetadata committed = offsets.get(partition);
            Assertions.assertThat(committed == null || committed.offset() < expectedOffset).isTrue();
        }
    }

    private void awaitCommittedOffset(TopicPartition partition, long expectedOffset) throws Exception {
        try (AdminClient adminClient = this.adminClient()) {
            Instant deadline = Instant.now().plusSeconds(15);
            while (Instant.now().isBefore(deadline)) {
                Map<TopicPartition, OffsetAndMetadata> offsets = adminClient
                        .listConsumerGroupOffsets(WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC)
                        .partitionsToOffsetAndMetadata()
                        .get(5, TimeUnit.SECONDS);
                OffsetAndMetadata committed = offsets.get(partition);
                if (committed != null && committed.offset() >= expectedOffset) {
                    return;
                }
                Thread.sleep(50L);
            }
        }
        Assertions.fail("Retry offset was not committed after recovered delivery");
    }

    private AdminClient adminClient() {
        return AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:" + System.getenv().getOrDefault("KAFKA_PORT", "9092")
        ));
    }

    private record CapturedRequest(String webhookId) {
    }

    private static final class TestReceiver implements AutoCloseable {

        private final HttpServer server;
        private final ExecutorService executor;
        private final ConcurrentLinkedQueue<CapturedRequest> requests = new ConcurrentLinkedQueue<>();
        private volatile CountDownLatch responseGate;
        private volatile CountDownLatch blockedResponse;

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
                throw new IllegalStateException("Could not start interrupted-wait receiver", exception);
            }
        }

        String uri() {
            return "http://localhost:" + this.server.getAddress().getPort() + "/webhook";
        }

        void blockResponses() {
            this.responseGate = new CountDownLatch(1);
            this.blockedResponse = new CountDownLatch(1);
        }

        void awaitBlockedResponse() throws InterruptedException {
            CountDownLatch blocked = this.blockedResponse;
            if (blocked == null || !blocked.await(15, TimeUnit.SECONDS)) {
                throw new AssertionError("Expected a webhook response to remain blocked");
            }
        }

        void releaseResponses() {
            CountDownLatch gate = this.responseGate;
            if (gate != null) {
                gate.countDown();
            }
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
                CountDownLatch blocked = this.blockedResponse;
                if (blocked != null) {
                    blocked.countDown();
                }
                CountDownLatch gate = this.responseGate;
                if (gate != null) {
                    gate.await(10, TimeUnit.SECONDS);
                }
                exchange.sendResponseHeaders(204, -1L);
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
