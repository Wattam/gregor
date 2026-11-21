package gregor.integration;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import gregor.avro.WebhookEvent;
import gregor.dto.WebhookRouteMessage;
import gregor.route.WebhookRoute;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
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
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@UseAdviceWith
@CamelSpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class RetryEligibilityWaitInterruptionIntegrationTests {

    private static final Instant INITIAL_INSTANT = Instant.parse("2026-07-25T12:00:00Z");
    private static final String TENANT_ID = "00000000-0000-0000-0000-000000000001";
    private static final String ENDPOINT_ID = "00000000-0000-0000-0000-000000000002";
    private static final String SIGNING_SECRET = "whsec_" + Base64.getEncoder().encodeToString("01234567890123456789012345678901".getBytes(StandardCharsets.UTF_8));
    private static final String RETRY_CONSUMER_URI = "kafka:" + WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC + "?groupId=" + WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC;
    private static final TestReceiver TEST_RECEIVER = TestReceiver.start();

    @Autowired
    private CamelContext camelContext;
    @Autowired
    private ProducerTemplate producerTemplate;
    @MockitoBean
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
        dynamicPropertyRegistry.add("camel.component.kafka.max-poll-interval-ms", () -> "4500000");
        dynamicPropertyRegistry.add("camel.component.kafka.request-required-acks", () -> "all");
        dynamicPropertyRegistry.add("camel.component.kafka.key-serializer", () -> "org.apache.kafka.common.serialization.StringSerializer");
        dynamicPropertyRegistry.add("camel.component.kafka.value-serializer", () -> "io.apicurio.registry.serde.avro.AvroKafkaSerializer");
        dynamicPropertyRegistry.add("camel.component.kafka.key-deserializer", () -> "org.apache.kafka.common.serialization.StringDeserializer");
        dynamicPropertyRegistry.add("camel.component.kafka.value-deserializer", () -> "io.apicurio.registry.serde.avro.AvroKafkaDeserializer");
        dynamicPropertyRegistry.add("camel.component.kafka.additional-properties[apicurio.registry.url]", () -> "http://localhost:" + registryPort + "/apis/registry/v3");
        dynamicPropertyRegistry.add("camel.component.kafka.additional-properties[apicurio.registry.auto-register]", () -> "true");
        dynamicPropertyRegistry.add("camel.component.kafka.additional-properties[apicurio.registry.use-specific-avro-reader]", () -> "true");
        dynamicPropertyRegistry.add("gregor.kafka.listener-concurrency", () -> "1");
        dynamicPropertyRegistry.add("gregor.webhook.tenant-id", () -> TENANT_ID);
        dynamicPropertyRegistry.add("gregor.webhook.endpoint-id", () -> ENDPOINT_ID);
        dynamicPropertyRegistry.add("gregor.webhook.receiver-url", TEST_RECEIVER::uri);
        dynamicPropertyRegistry.add("gregor.webhook.signing-secret", () -> SIGNING_SECRET);
        dynamicPropertyRegistry.add("gregor.webhook.connection-timeout", () -> "2s");
        dynamicPropertyRegistry.add("gregor.webhook.response-timeout", () -> "5s");
        dynamicPropertyRegistry.add("gregor.webhook.receiver-timestamp-tolerance", () -> "5m");
    }

    @AfterAll
    static void stopReceiver() {
        TEST_RECEIVER.close();
    }

    @Test
    void interruptsAWaitThenRedeliversAndCommitsOnlyAfterRecoveredDelivery() throws Exception {
        AtomicReference<Instant> currentInstantReference = new AtomicReference<>(INITIAL_INSTANT);
        Mockito.when(this.clock.instant()).thenAnswer(_ -> currentInstantReference.get());
        Mockito.when(this.clock.getZone()).thenReturn(ZoneOffset.UTC);
        String eventId = UUID.randomUUID().toString();
        CountDownLatch eligibilityStartedCountDownLatch = new CountDownLatch(1);
        AdviceWith.adviceWith(
                this.camelContext,
                WebhookRoute.WEBHOOK_RETRY_ELIGIBILITY_DIRECT,
                adviceWithRouteBuilder -> adviceWithRouteBuilder.weaveAddFirst().process(exchange -> {
                    WebhookRouteMessage webhookRouteMessage = exchange.getMessage().getBody(WebhookRouteMessage.class);
                    if (Objects.equals(eventId, webhookRouteMessage.webhookEvent().getEventId())) {
                        eligibilityStartedCountDownLatch.countDown();
                    }
                })
        );
        this.camelContext.start();
        this.camelContext.getRouteController().startRoute(WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT);
        this.camelContext.getRouteController().startRoute(WebhookRoute.WEBHOOK_DELIVERY_ATTEMPT_DIRECT);
        this.camelContext.getRouteController().startRoute(WebhookRoute.WEBHOOK_RETRY_ELIGIBILITY_DIRECT);
        this.camelContext.getRouteController().startRoute(RETRY_CONSUMER_URI);
        TEST_RECEIVER.blockResponse(eventId);

        Instant nextEligibleAt = INITIAL_INSTANT.plusSeconds(4_200);
        WebhookEvent webhookEvent = new WebhookEvent(eventId, "interrupted-eligibility.test", TENANT_ID, ENDPOINT_ID, INITIAL_INSTANT, ByteBuffer.wrap("{\"interrupted\":true}".getBytes(StandardCharsets.UTF_8)), 1, nextEligibleAt, 1, 2);
        this.producerTemplate.sendBodyAndHeaders("kafka:" + WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC, webhookEvent, Map.of(KafkaConstants.KEY, ENDPOINT_ID, KafkaConstants.PARTITION_KEY, 0));
        TopicPartition topicPartition = new TopicPartition(WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC, 0);
        long expectedOffset = this.endOffset(topicPartition);

        Assertions.assertThat(eligibilityStartedCountDownLatch.await(15, TimeUnit.SECONDS)).isTrue();
        Assertions.assertThat(TEST_RECEIVER.count(eventId)).isZero();
        this.assertOffsetUncommitted(topicPartition, expectedOffset);
        boolean stopped = this.camelContext.getRouteController().stopRoute(RETRY_CONSUMER_URI, 1, TimeUnit.SECONDS, false);
        Assertions.assertThat(stopped).isTrue();
        Assertions.assertThat(this.camelContext.getRouteController().getRouteStatus(RETRY_CONSUMER_URI).isStopped()).isTrue();

        currentInstantReference.set(nextEligibleAt);
        this.camelContext.getRouteController().startRoute(RETRY_CONSUMER_URI);
        TEST_RECEIVER.awaitBlockedResponse();
        this.assertOffsetUncommitted(topicPartition, expectedOffset);

        TEST_RECEIVER.releaseResponses();
        this.awaitCommittedOffset(topicPartition, expectedOffset);
        Assertions.assertThat(TEST_RECEIVER.count(eventId)).isEqualTo(1);
    }

    private long endOffset(TopicPartition topicPartition) throws Exception {
        try (AdminClient adminClient = this.adminClient()) {
            return adminClient.listOffsets(Map.of(topicPartition, OffsetSpec.latest())).partitionResult(topicPartition).get(5, TimeUnit.SECONDS).offset();
        }
    }

    private void assertOffsetUncommitted(TopicPartition topicPartition, long expectedOffset) throws Exception {
        try (AdminClient adminClient = this.adminClient()) {
            Map<TopicPartition, OffsetAndMetadata> offsets = adminClient.listConsumerGroupOffsets(WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
            OffsetAndMetadata committedOffsetAndMetadata = offsets.get(topicPartition);
            Assertions.assertThat(Objects.isNull(committedOffsetAndMetadata) || committedOffsetAndMetadata.offset() < expectedOffset).isTrue();
        }
    }

    private void awaitCommittedOffset(TopicPartition topicPartition, long expectedOffset) throws Exception {
        try (AdminClient adminClient = this.adminClient()) {
            Instant deadline = Instant.now().plusSeconds(15);
            while (Instant.now().isBefore(deadline)) {
                Map<TopicPartition, OffsetAndMetadata> offsets = adminClient.listConsumerGroupOffsets(WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
                OffsetAndMetadata committedOffsetAndMetadata = offsets.get(topicPartition);
                if (Objects.nonNull(committedOffsetAndMetadata) && committedOffsetAndMetadata.offset() >= expectedOffset) {
                    return;
                }
                Thread.sleep(50L);
            }
        }
        Assertions.fail("Retry offset was not committed after recovered delivery");
    }

    private AdminClient adminClient() {
        return AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + System.getenv().getOrDefault("KAFKA_PORT", "9092")));
    }

    private record CapturedRequest(String webhookId) {
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static final class TestReceiver implements AutoCloseable {

        private final HttpServer httpServer;
        private final ExecutorService executorService;
        private final ConcurrentLinkedQueue<CapturedRequest> capturedRequests = new ConcurrentLinkedQueue<>();
        private volatile CountDownLatch responseCountDownLatch;
        private volatile CountDownLatch responseBlockedCountDownLatch;
        private volatile String blockedEventId;

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
                throw new IllegalStateException("Could not start interrupted-wait receiver", exception);
            }
        }

        String uri() {
            return "http://localhost:" + this.httpServer.getAddress().getPort() + "/webhook";
        }

        void blockResponse(String eventId) {
            this.responseCountDownLatch = new CountDownLatch(1);
            this.responseBlockedCountDownLatch = new CountDownLatch(1);
            this.blockedEventId = eventId;
        }

        void awaitBlockedResponse() throws InterruptedException {
            CountDownLatch responseBlockedCountDownLatch = this.responseBlockedCountDownLatch;
            if (Objects.isNull(responseBlockedCountDownLatch) || !responseBlockedCountDownLatch.await(15, TimeUnit.SECONDS)) {
                throw new AssertionError("Expected a webhook response to remain blocked");
            }
        }

        void releaseResponses() {
            CountDownLatch responseCountDownLatch = this.responseCountDownLatch;
            if (Objects.nonNull(responseCountDownLatch)) {
                responseCountDownLatch.countDown();
            }
        }

        int count(String eventId) {
            return (int) this.capturedRequests.stream().filter(capturedRequest -> Objects.equals(eventId, capturedRequest.webhookId())).count();
        }

        private void handle(HttpExchange httpExchange) throws IOException {
            httpExchange.getRequestBody().readAllBytes();
            String eventId = httpExchange.getRequestHeaders().getFirst("webhook-id");
            this.capturedRequests.add(new CapturedRequest(eventId));
            try (httpExchange) {
                if (Objects.nonNull(eventId) && Objects.equals(eventId, this.blockedEventId)) {
                    this.responseBlockedCountDownLatch.countDown();
                    this.responseCountDownLatch.await(10, TimeUnit.SECONDS);
                }
                httpExchange.sendResponseHeaders(204, -1L);
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
    }
}
