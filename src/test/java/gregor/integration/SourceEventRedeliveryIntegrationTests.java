package gregor.integration;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import gregor.avro.DeliveryAttempt;
import gregor.avro.WebhookEvent;
import gregor.component.SignatureComponent;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@UseAdviceWith
@CamelSpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class SourceEventRedeliveryIntegrationTests {

    private static final String SIGNING_SECRET = "whsec_" + Base64.getEncoder().encodeToString(
            "01234567890123456789012345678901".getBytes(StandardCharsets.UTF_8)
    );
    private static final String WEBHOOK_DELIVERY_URI =
            "kafka:" + WebhookRoute.WEBHOOK_EVENT_TOPIC + "?groupId=" + WebhookRoute.WEBHOOK_EVENT_TOPIC;
    private static final TestReceiver RECEIVER = TestReceiver.start();

    @Autowired
    private CamelContext camelContext;
    @Autowired
    private ProducerTemplate producerTemplate;
    @Autowired
    private SignatureComponent signatureService;

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
        registry.add("gregor.kafka.listener-concurrency", () -> "1");
        registry.add("gregor.webhook.tenant-id", () -> "00000000-0000-0000-0000-000000000001");
        registry.add("gregor.webhook.endpoint-id", () -> "00000000-0000-0000-0000-000000000002");
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
    void redeliversWithSameWebhookIdAfterManualCommitIsWithheldFollowingHttpPost() throws Exception {
        String eventId = UUID.randomUUID().toString();
        byte[] payload = "{\"redelivery\":true}".getBytes(StandardCharsets.UTF_8);
        WebhookEvent event = new WebhookEvent(
                eventId,
                "redelivery.test",
                "00000000-0000-0000-0000-000000000001",
                "00000000-0000-0000-0000-000000000002",
                Instant.now(),
                ByteBuffer.wrap(payload),
                0,
                null,
                0,
                0
        );
        AtomicInteger publicationCount = new AtomicInteger();
        ConcurrentLinkedQueue<DeliveryAttempt> attempts = new ConcurrentLinkedQueue<>();
        AdviceWith.adviceWith(
                this.camelContext,
                WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT,
                advice -> advice.weaveByToUri("kafka:" + WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC + "*")
                        .before()
                        .process(exchange -> {
                            DeliveryAttempt attempt = exchange.getMessage().getBody(DeliveryAttempt.class);
                            if (eventId.equals(attempt.getEventId())) {
                                attempts.add(attempt);
                                if (publicationCount.incrementAndGet() == 1) {
                                    throw new IllegalStateException("delivery-attempt publication unavailable");
                                }
                            }
                        })
        );
        this.camelContext.start();
        this.camelContext.getRouteController().startRoute(WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT);
        this.camelContext.getRouteController().startRoute(WebhookRoute.WEBHOOK_DELIVERY_ATTEMPT_DIRECT);
        this.camelContext.getRouteController().startRoute(WEBHOOK_DELIVERY_URI);
        RECEIVER.blockResponses();

        this.producerTemplate.sendBodyAndHeaders(
                "kafka:" + WebhookRoute.WEBHOOK_EVENT_TOPIC,
                event,
                Map.of(KafkaConstants.KEY, event.getEndpointId(), KafkaConstants.PARTITION_KEY, 0)
        );
        TopicPartition partition = new TopicPartition(WebhookRoute.WEBHOOK_EVENT_TOPIC, 0);
        long offsetAfterEvent = this.endOffset(partition);

        RECEIVER.awaitRequest(eventId, 1);
        RECEIVER.awaitBlockedResponse();
        this.assertOffsetUncommitted(partition, offsetAfterEvent);
        RECEIVER.releaseResponseAndBlockNext();

        RECEIVER.awaitRequest(eventId, 2);
        RECEIVER.awaitBlockedResponse();
        this.awaitPublicationCount(publicationCount, 1);
        this.assertOffsetUncommitted(partition, offsetAfterEvent);
        RECEIVER.releaseResponses();

        this.awaitPublicationCount(publicationCount, 2);
        this.awaitCommittedOffset(partition, offsetAfterEvent);
        Assertions.assertThat(RECEIVER.requests(eventId))
                .hasSize(2)
                .allSatisfy(request -> {
                    Assertions.assertThat(request.webhookId()).isEqualTo(eventId);
                    Assertions.assertThat(request.body()).isEqualTo(payload);
                    Assertions.assertThat(request.signature()).isEqualTo(this.signatureService.sign(
                            request.webhookId(),
                            Long.parseLong(request.timestamp()),
                            request.body()
                    ));
                });
        Assertions.assertThat(attempts).hasSize(2).allSatisfy(attempt -> {
            Assertions.assertThat(attempt.getAttemptNumber()).isEqualTo(1);
            Assertions.assertThat(attempt.getRetryCount()).isZero();
            Assertions.assertThat(attempt.getReplayCount()).isZero();
            Assertions.assertThat(attempt.getDeadLettered()).isFalse();
        });
    }

    private void awaitPublicationCount(AtomicInteger publicationCount, int expectedCount) throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(15);
        while (Instant.now().isBefore(deadline)) {
            if (publicationCount.get() >= expectedCount) {
                return;
            }
            Thread.sleep(50L);
        }
        Assertions.fail("Expected " + expectedCount + " delivery-attempt publication calls");
    }

    private long endOffset(TopicPartition partition) throws Exception {
        try (AdminClient adminClient = this.adminClient()) {
            return adminClient.listOffsets(Map.of(partition, OffsetSpec.latest()))
                    .partitionResult(partition)
                    .get(5, TimeUnit.SECONDS)
                    .offset();
        }
    }

    private void assertOffsetUncommitted(TopicPartition partition, long offsetAfterEvent) throws Exception {
        try (AdminClient adminClient = this.adminClient()) {
            Map<TopicPartition, OffsetAndMetadata> offsets = adminClient
                    .listConsumerGroupOffsets(WebhookRoute.WEBHOOK_EVENT_TOPIC)
                    .partitionsToOffsetAndMetadata()
                    .get(5, TimeUnit.SECONDS);
            OffsetAndMetadata committed = offsets.get(partition);
            Assertions.assertThat(committed == null || committed.offset() < offsetAfterEvent).isTrue();
        }
    }

    private void awaitCommittedOffset(TopicPartition partition, long expectedOffset) throws Exception {
        try (AdminClient adminClient = this.adminClient()) {
            Instant deadline = Instant.now().plusSeconds(15);
            while (Instant.now().isBefore(deadline)) {
                Map<TopicPartition, OffsetAndMetadata> offsets = adminClient
                        .listConsumerGroupOffsets(WebhookRoute.WEBHOOK_EVENT_TOPIC)
                        .partitionsToOffsetAndMetadata()
                        .get(5, TimeUnit.SECONDS);
                OffsetAndMetadata committed = offsets.get(partition);
                if (committed != null && committed.offset() >= expectedOffset) {
                    return;
                }
                Thread.sleep(50L);
            }
        }
        Assertions.fail("Source offset was not eventually committed after redelivery");
    }

    private AdminClient adminClient() {
        return AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:" + System.getenv().getOrDefault("KAFKA_PORT", "9092")
        ));
    }

    private record CapturedRequest(String webhookId, String timestamp, String signature, byte[] body) {
    }

    private static final class TestReceiver implements AutoCloseable {

        private final HttpServer server;
        private final ExecutorService executor;
        private final ConcurrentLinkedQueue<CapturedRequest> requests = new ConcurrentLinkedQueue<>();
        private volatile ResponseBlock responseBlock;

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
                throw new IllegalStateException("Could not start source-redelivery receiver", exception);
            }
        }

        String uri() {
            return "http://localhost:" + this.server.getAddress().getPort() + "/webhook";
        }

        void blockResponses() {
            this.responseBlock = new ResponseBlock(new CountDownLatch(1), new CountDownLatch(1));
        }

        void awaitBlockedResponse() throws InterruptedException {
            ResponseBlock block = this.responseBlock;
            if (block == null || !block.blocked().await(15, TimeUnit.SECONDS)) {
                throw new AssertionError("Expected a webhook response to remain blocked");
            }
        }

        void releaseResponseAndBlockNext() {
            ResponseBlock current = this.responseBlock;
            this.responseBlock = new ResponseBlock(new CountDownLatch(1), new CountDownLatch(1));
            if (current != null) {
                current.gate().countDown();
            }
        }

        void releaseResponses() {
            ResponseBlock block = this.responseBlock;
            if (block != null) {
                block.gate().countDown();
            }
        }

        int count(String eventId) {
            return this.requests(eventId).size();
        }

        List<CapturedRequest> requests(String eventId) {
            return this.requests.stream()
                    .filter(request -> eventId.equals(request.webhookId()))
                    .toList();
        }

        void awaitRequest(String eventId, int expectedCount) throws InterruptedException {
            Instant deadline = Instant.now().plusSeconds(15);
            while (Instant.now().isBefore(deadline)) {
                if (this.count(eventId) >= expectedCount) {
                    return;
                }
                Thread.sleep(25L);
            }
            throw new AssertionError("Expected " + expectedCount + " requests for event " + eventId);
        }

        private void handle(HttpExchange exchange) throws IOException {
            byte[] body = exchange.getRequestBody().readAllBytes();
            this.requests.add(new CapturedRequest(
                    exchange.getRequestHeaders().getFirst("webhook-id"),
                    exchange.getRequestHeaders().getFirst("webhook-timestamp"),
                    exchange.getRequestHeaders().getFirst("webhook-signature"),
                    body
            ));
            try (exchange) {
                ResponseBlock block = this.responseBlock;
                if (block != null) {
                    block.blocked().countDown();
                    block.gate().await(10, TimeUnit.SECONDS);
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

        private record ResponseBlock(CountDownLatch gate, CountDownLatch blocked) {
        }
    }
}
