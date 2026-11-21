package gregor.integration;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import gregor.avro.WebhookEvent;
import gregor.route.WebhookRoute;
import io.vertx.core.Vertx;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
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
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.mvc.method.RequestMappingInfo;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "gregor.webhook.maximum-attempts=1")
class MaximumAttemptsOneIntegrationTests {

    private static final String DEAD_LETTER_TOPIC = "webhook-events-dead-letter";
    private static final TestReceiver TEST_RECEIVER = TestReceiver.start();

    @LocalServerPort
    private int serverPort;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private Vertx registryVertx;
    @Autowired
    private RequestMappingHandlerMapping requestMappingHandlerMapping;

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
        dynamicPropertyRegistry.add("camel.main.auto-startup", () -> "true");
        dynamicPropertyRegistry.add("camel.component.kafka.request-required-acks", () -> "all");
        dynamicPropertyRegistry.add("camel.component.kafka.key-serializer", () -> "org.apache.kafka.common.serialization.StringSerializer");
        dynamicPropertyRegistry.add("camel.component.kafka.value-serializer", () -> "io.apicurio.registry.serde.avro.AvroKafkaSerializer");
        dynamicPropertyRegistry.add("camel.component.kafka.key-deserializer", () -> "org.apache.kafka.common.serialization.StringDeserializer");
        dynamicPropertyRegistry.add("camel.component.kafka.value-deserializer", () -> "io.apicurio.registry.serde.avro.AvroKafkaDeserializer");
        dynamicPropertyRegistry.add("camel.component.kafka.additional-properties[apicurio.registry.url]", () -> "http://localhost:" + registryPort + "/apis/registry/v3");
        dynamicPropertyRegistry.add("camel.component.kafka.additional-properties[apicurio.registry.auto-register]", () -> "true");
        dynamicPropertyRegistry.add("camel.component.kafka.additional-properties[apicurio.registry.use-specific-avro-reader]", () -> "true");
        dynamicPropertyRegistry.add("gregor.kafka.listener-concurrency", () -> "1");
        dynamicPropertyRegistry.add("gregor.webhook.tenant-id", () -> "00000000-0000-0000-0000-000000000001");
        dynamicPropertyRegistry.add("gregor.webhook.endpoint-id", () -> "00000000-0000-0000-0000-000000000002");
        dynamicPropertyRegistry.add("gregor.webhook.receiver-url", TEST_RECEIVER::uri);
        dynamicPropertyRegistry.add("gregor.webhook.signing-secret", () -> "whsec_MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE=");
        dynamicPropertyRegistry.add("gregor.webhook.connection-timeout", () -> "2s");
        dynamicPropertyRegistry.add("gregor.webhook.response-timeout", () -> "5s");
        dynamicPropertyRegistry.add("gregor.webhook.receiver-timestamp-tolerance", () -> "5m");
    }

    @AfterAll
    static void stopReceiver() {
        TEST_RECEIVER.close();
    }

    @Test
    void configuredMaximumOfOneDeadLettersTheInitialRetryableFailure() throws Exception {
        try (AdminClient adminClient = KafkaTestRecords.adminClient()) {
            Map<TopicPartition, Long> retryOffsets = KafkaTestRecords.retryEndOffsets(adminClient);
            Map<TopicPartition, Long> expectedDeadLetterOffsets = new HashMap<>(KafkaTestRecords.endOffsets(adminClient, DEAD_LETTER_TOPIC));
            try (
                    KafkaConsumer<String, Object> sourceKafkaConsumer = KafkaTestRecords.consumer(this.registryVertx, KafkaTestRecords.endOffsets(adminClient, WebhookRoute.WEBHOOK_EVENT_TOPIC));
                    KafkaConsumer<String, Object> deadLetterKafkaConsumer = KafkaTestRecords.consumer(this.registryVertx, expectedDeadLetterOffsets)
            ) {
                String eventId = this.postEvent();
                ConsumerRecord<String, Object> inputConsumerRecord = KafkaTestRecords.awaitRecord(sourceKafkaConsumer, eventId);
                KafkaTestRecords.awaitCommit(adminClient, inputConsumerRecord);
                Assertions.assertThat(TEST_RECEIVER.count(eventId)).isEqualTo(1);
                WebhookEvent inputWebhookEvent = (WebhookEvent) inputConsumerRecord.value();
                ConsumerRecord<String, Object> deadLetterConsumerRecord = KafkaTestRecords.awaitRecord(deadLetterKafkaConsumer, eventId);
                Assertions.assertThat(deadLetterConsumerRecord.topic()).isEqualTo(DEAD_LETTER_TOPIC);
                Assertions.assertThat(deadLetterConsumerRecord.key()).isEqualTo(inputWebhookEvent.getEndpointId());
                Assertions.assertThat(deadLetterConsumerRecord.value()).isEqualTo(WebhookEvent.newBuilder(inputWebhookEvent).setDeliveryAttemptCount(1).build());
                Assertions.assertThat(((WebhookEvent) deadLetterConsumerRecord.value()).getNextEligibleAt()).isNull();
                expectedDeadLetterOffsets.compute(new TopicPartition(DEAD_LETTER_TOPIC, deadLetterConsumerRecord.partition()), (_, offset) -> offset + 1);
                Assertions.assertThat(KafkaTestRecords.endOffsets(adminClient, DEAD_LETTER_TOPIC)).isEqualTo(expectedDeadLetterOffsets);
                Assertions.assertThat(this.awaitAttemptRow(eventId))
                        .containsEntry("attempt_number", 1)
                        .containsEntry("outcome", "RETRYABLE_FAILURE")
                        .containsEntry("http_status", 503)
                        .containsEntry("retry_count", 0)
                        .containsEntry("replay_count", 0)
                        .containsEntry("dead_lettered", true);
                Assertions.assertThat(KafkaTestRecords.retryEndOffsets(adminClient)).isEqualTo(retryOffsets);
                Assertions.assertThat(this.jdbcTemplate.queryForObject("SELECT count(*) FROM dead_letter_occurrence WHERE event_id = ?::uuid", Integer.class, eventId)).isZero();
            }
        }
    }

    @Test
    void exposesOnlyTheTwoExistingApplicationHttpEndpoints() {
        List<RequestMappingInfo> requestMappingInfos = this.requestMappingHandlerMapping
                .getHandlerMethods()
                .entrySet()
                .stream()
                .filter(entry -> StringUtils.startsWith(entry.getValue().getBeanType().getPackageName(), "gregor."))
                .map(Map.Entry::getKey)
                .toList();
        Assertions.assertThat(requestMappingInfos)
                .extracting(requestMappingInfo -> Assertions.tuple(requestMappingInfo.getPatternValues(), requestMappingInfo.getMethodsCondition().getMethods()))
                .containsExactlyInAnyOrder(
                        Assertions.tuple(Set.of("/api/events"), Set.of(RequestMethod.POST)),
                        Assertions.tuple(Set.of("/api/mocked-webhook-receiver"), Set.of(RequestMethod.POST))
                );
    }

    private String postEvent() throws Exception {
        HttpRequest httpRequest = HttpRequest
                .newBuilder()
                .uri(URI.create("http://localhost:" + this.serverPort + "/api/events"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"eventType\":\"single-attempt.test\",\"payload\":{}}", StandardCharsets.UTF_8))
                .build();

        HttpResponse<String> httpResponse = HttpClient.newHttpClient().send(httpRequest, HttpResponse.BodyHandlers.ofString());

        Assertions.assertThat(httpResponse.statusCode()).isEqualTo(202);
        return this.objectMapper.readTree(httpResponse.body()).get("eventId").asString();
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
        Integer count = this.jdbcTemplate.queryForObject("SELECT count(*) FROM delivery_attempt WHERE event_id = ?::uuid", Integer.class, eventId);
        return Objects.isNull(count) ? 0 : count;
    }

    private record CapturedRequest(String webhookId) {
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static final class TestReceiver implements AutoCloseable {

        private final HttpServer httpServer;
        private final ExecutorService executorService;
        private final ConcurrentLinkedQueue<CapturedRequest> capturedRequests = new ConcurrentLinkedQueue<>();

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
                throw new IllegalStateException("Could not start maximum-attempts receiver", exception);
            }
        }

        String uri() {
            return "http://localhost:" + this.httpServer.getAddress().getPort() + "/webhook";
        }

        int count(String eventId) {
            return (int) this.capturedRequests.stream().filter(capturedRequest -> Objects.equals(eventId, capturedRequest.webhookId())).count();
        }

        private void handle(HttpExchange httpExchange) throws IOException {
            httpExchange.getRequestBody().readAllBytes();
            this.capturedRequests.add(new CapturedRequest(httpExchange.getRequestHeaders().getFirst("webhook-id")));
            try (httpExchange) {
                httpExchange.sendResponseHeaders(503, -1L);
            }
        }

        @Override
        public void close() {
            this.httpServer.stop(0);
            this.executorService.shutdownNow();
        }
    }
}
