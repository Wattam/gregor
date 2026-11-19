package gregor.integration;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import gregor.route.WebhookRoute;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "gregor.webhook.maximum-attempts=1"
)
class MaximumAttemptsOneIntegrationTests {

    private static final TestReceiver RECEIVER = TestReceiver.start();

    @LocalServerPort
    private int serverPort;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private Vertx registryVertx;

    @DynamicPropertySource
    static void configureInfrastructure(DynamicPropertyRegistry dynamicPropertyRegistry) {
        String postgresHost = System.getenv().getOrDefault("POSTGRES_HOST", "localhost");
        String postgresPort = System.getenv().getOrDefault("POSTGRES_PORT", "5432");
        String postgresDatabase = System.getenv().getOrDefault("POSTGRES_DB", "gregor");
        String kafkaPort = System.getenv().getOrDefault("KAFKA_PORT", "9092");
        String registryPort = System.getenv().getOrDefault("SCHEMA_REGISTRY_PORT", "8081");

        dynamicPropertyRegistry.add(
                "spring.datasource.url",
                () -> "jdbc:postgresql://" + postgresHost + ":" + postgresPort + "/" + postgresDatabase
        );
        dynamicPropertyRegistry.add("spring.datasource.username", () -> System.getenv()
                .getOrDefault("POSTGRES_USER", "gregor"));
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
        dynamicPropertyRegistry.add("camel.main.auto-startup", () -> "true");
        dynamicPropertyRegistry.add("camel.component.kafka.request-required-acks", () -> "all");
        dynamicPropertyRegistry.add(
                "camel.component.kafka.key-serializer",
                () -> "org.apache.kafka.common.serialization.StringSerializer"
        );
        dynamicPropertyRegistry.add(
                "camel.component.kafka.value-serializer",
                () -> "io.apicurio.registry.serde.avro.AvroKafkaSerializer"
        );
        dynamicPropertyRegistry.add(
                "camel.component.kafka.key-deserializer",
                () -> "org.apache.kafka.common.serialization.StringDeserializer"
        );
        dynamicPropertyRegistry.add(
                "camel.component.kafka.value-deserializer",
                () -> "io.apicurio.registry.serde.avro.AvroKafkaDeserializer"
        );
        dynamicPropertyRegistry.add(
                "camel.component.kafka.additional-properties[apicurio.registry.url]",
                () -> "http://localhost:" + registryPort + "/apis/registry/v3"
        );
        dynamicPropertyRegistry.add("camel.component.kafka.additional-properties[apicurio.registry.auto-register]", () -> "true");
        dynamicPropertyRegistry.add(
                "camel.component.kafka.additional-properties[apicurio.registry.use-specific-avro-reader]",
                () -> "true"
        );
        dynamicPropertyRegistry.add("gregor.kafka.listener-concurrency", () -> "1");
        dynamicPropertyRegistry.add("gregor.webhook.tenant-id", () -> "00000000-0000-0000-0000-000000000001");
        dynamicPropertyRegistry.add("gregor.webhook.endpoint-id", () -> "00000000-0000-0000-0000-000000000002");
        dynamicPropertyRegistry.add("gregor.webhook.receiver-url", RECEIVER::uri);
        dynamicPropertyRegistry.add(
                "gregor.webhook.signing-secret",
                () -> "whsec_MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE="
        );
        dynamicPropertyRegistry.add("gregor.webhook.connection-timeout", () -> "2s");
        dynamicPropertyRegistry.add("gregor.webhook.response-timeout", () -> "5s");
        dynamicPropertyRegistry.add("gregor.webhook.receiver-timestamp-tolerance", () -> "5m");
    }

    @AfterAll
    static void stopReceiver() {
        RECEIVER.close();
    }

    @Test
    void configuredMaximumOfOnePermitsOnlyTheInitialAttempt() throws Exception {
        String[] delayedTopics = {
                WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC, WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC,
                WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC, WebhookRoute.WEBHOOK_EVENT_DEAD_LETTER_TOPIC
        };
        try (AdminClient admin = KafkaTestRecords.adminClient()) {
            Map<TopicPartition, Long> before = KafkaTestRecords.endOffsets(admin, delayedTopics);
            try (
                    KafkaConsumer<String, Object> source = KafkaTestRecords.consumer(
                            this.registryVertx, KafkaTestRecords.endOffsets(admin, WebhookRoute.WEBHOOK_EVENT_TOPIC)
                    )
            ) {
                String eventId = this.postEvent();
                ConsumerRecord<String, Object> input = KafkaTestRecords.awaitRecord(source, eventId);
                KafkaTestRecords.awaitCommit(admin, input);
                Assertions.assertThat(RECEIVER.count(eventId)).isEqualTo(1);
                Assertions.assertThat(this.awaitAttemptRow(eventId))
                        .containsEntry("attempt_number", 1)
                        .containsEntry("outcome", "RETRYABLE_FAILURE")
                        .containsEntry("http_status", 503)
                        .containsEntry("retry_count", 0)
                        .containsEntry("replay_count", 0)
                        .containsEntry("dead_lettered", false);
                Assertions.assertThat(KafkaTestRecords.endOffsets(admin, delayedTopics)).isEqualTo(before);
            }
        }
    }

    private String postEvent() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + this.serverPort + "/api/events"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"eventType\":\"single-attempt.test\",\"payload\":{}}",
                        StandardCharsets.UTF_8
                ))
                .build();

        HttpResponse<String> response = HttpClient.newHttpClient()
                .send(request, HttpResponse.BodyHandlers.ofString());

        Assertions.assertThat(response.statusCode()).isEqualTo(202);
        return this.objectMapper.readTree(response.body()).get("eventId").asString();
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

    private record CapturedRequest(String webhookId) {
    }

    private static final class TestReceiver implements AutoCloseable {

        private final HttpServer httpServer;
        private final ExecutorService executor;
        private final ConcurrentLinkedQueue<CapturedRequest> requests = new ConcurrentLinkedQueue<>();

        private TestReceiver(HttpServer httpServer, ExecutorService executorService) {
            this.httpServer = httpServer;
            this.executor = executorService;
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
                throw new IllegalStateException("Could not start maximum-attempts receiver", exception);
            }
        }

        String uri() {
            return "http://localhost:" + this.httpServer.getAddress().getPort() + "/webhook";
        }

        int count(String eventId) {
            return (int) this.requests.stream().filter(request -> eventId.equals(request.webhookId())).count();
        }

        private void handle(HttpExchange exchange) throws IOException {
            exchange.getRequestBody().readAllBytes();
            this.requests.add(new CapturedRequest(exchange.getRequestHeaders().getFirst("webhook-id")));
            try (exchange) {
                exchange.sendResponseHeaders(503, -1L);
            }
        }

        @Override
        public void close() {
            this.httpServer.stop(0);
            this.executor.shutdownNow();
        }
    }
}
