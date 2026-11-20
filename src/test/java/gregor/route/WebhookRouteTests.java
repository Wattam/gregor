package gregor.route;

import gregor.avro.DeliveryAttempt;
import gregor.avro.WebhookEvent;
import gregor.component.RestComponent;
import gregor.config.properties.WebhookProperties;
import gregor.dto.DeliveryAttemptResult;
import gregor.dto.DeliveryOutcome;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.apache.camel.test.spring.junit5.UseAdviceWith;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import tools.jackson.databind.json.JsonMapper;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@UseAdviceWith
@CamelSpringBootTest
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = "gregor.webhook.maximum-attempts=4")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class WebhookRouteTests {

    private static final String INPUT = "direct:delivery-input";
    private static final String ATTEMPT_OUTPUT = "mock:delivery-attempt";
    private static final String RETRY_OUTPUT = "mock:retry-event";
    private static final String DEAD_LETTER_OUTPUT = "mock:dead-letter";

    @Autowired
    private CamelContext camelContext;
    @Autowired
    private ProducerTemplate producerTemplate;
    @MockitoBean
    private RestComponent restComponent;
    @MockitoSpyBean
    private WebhookProperties webhookProperties;
    @MockitoSpyBean
    private JsonMapper jsonMapper;

    @ParameterizedTest
    @CsvSource({
            "webhook-events, , 4, 101, RETRYABLE_FAILURE, 0, 0, NONE, webhook-events-retry-1m, HTTP/attempt/retry/commit",
            "webhook-events, , 1, 101, RETRYABLE_FAILURE, 0, 0, NONE, , HTTP/dead-letter/attempt/commit",
            "webhook-events, , 4, 408, RETRYABLE_FAILURE, 0, 0, NONE, webhook-events-retry-1m, HTTP/attempt/retry/commit",
            "webhook-events-retry-1m, 1970-01-01T00:01:00Z, 4, 429, RETRYABLE_FAILURE, 1, 1, NONE, webhook-events-retry-10m, HTTP/attempt/retry/commit",
            "webhook-events-retry-1m, 1970-01-01T00:01:00Z, 4, 503, RETRYABLE_FAILURE, 1, 2, NONE, webhook-events-retry-1h, HTTP/attempt/retry/commit",
            "webhook-events, , 4, 503, RETRYABLE_FAILURE, 0, 0, ATTEMPT, , HTTP/attempt",
            "webhook-events-retry-1m, 1970-01-01T00:01:00Z, 4, 503, RETRYABLE_FAILURE, 1, 2, ATTEMPT, , HTTP/attempt",
            "webhook-events, , 4, 503, RETRYABLE_FAILURE, 0, 0, RETRY, webhook-events-retry-1m, HTTP/attempt/retry",
            "webhook-events-retry-1m, 1970-01-01T00:01:00Z, 4, 503, RETRYABLE_FAILURE, 1, 2, RETRY, webhook-events-retry-1h, HTTP/attempt/retry",
            "webhook-events, , 4, 204, SUCCEEDED, 0, 0, NONE, , HTTP/attempt/commit",
            "webhook-events-retry-1h, 1970-01-01T00:01:00Z, 4, 299, SUCCEEDED, 3, 3, NONE, , HTTP/attempt/commit",
            "webhook-events, , 4, 400, PERMANENT_FAILURE, 0, 0, NONE, , HTTP/dead-letter/attempt/commit",
            "webhook-events, 1970-01-01T00:01:00Z, 4, 400, PERMANENT_FAILURE, 0, 0, NONE, , HTTP/dead-letter/attempt/commit",
            "webhook-events-retry-1m, 1970-01-01T00:01:00Z, 4, 302, PERMANENT_FAILURE, 1, 1, NONE, , HTTP/dead-letter/attempt/commit",
            "webhook-events, , 1, 503, RETRYABLE_FAILURE, 0, 0, NONE, , HTTP/dead-letter/attempt/commit",
            "webhook-events-retry-1m, 1970-01-01T00:01:00Z, 2, 503, RETRYABLE_FAILURE, 1, 1, NONE, , HTTP/dead-letter/attempt/commit",
            "webhook-events-retry-10m, 1970-01-01T00:01:00Z, 3, 408, RETRYABLE_FAILURE, 2, 2, NONE, , HTTP/dead-letter/attempt/commit",
            "webhook-events-retry-1h, 1970-01-01T00:01:00Z, 4, 429, RETRYABLE_FAILURE, 3, 3, NONE, , HTTP/dead-letter/attempt/commit",
            "webhook-events, , 4, 503, RETRYABLE_FAILURE, 0, 3, NONE, , HTTP/dead-letter/attempt/commit",
            "webhook-events-retry-1m, 1970-01-01T00:01:00Z, 4, 503, RETRYABLE_FAILURE, 1, 7, NONE, , HTTP/dead-letter/attempt/commit",
            "webhook-events, , 4, 400, PERMANENT_FAILURE, 0, 0, DEAD_LETTER, , HTTP/dead-letter",
            "webhook-events, , 4, 400, PERMANENT_FAILURE, 0, 0, ATTEMPT_DETAILS, , HTTP/dead-letter",
            "webhook-events-retry-1h, 1970-01-01T00:01:00Z, 4, 503, RETRYABLE_FAILURE, 3, 3, ATTEMPT, , HTTP/dead-letter/attempt"
    })
    void publishesOnceInOrderAndCommitsOnlyAfterRequiredSends(
            String topic,
            Instant nextEligibleAt,
            int maximumAttempts,
            int httpStatus,
            DeliveryOutcome deliveryOutcome,
            int priorAttempts,
            int retryCount,
            FailedPublication failedPublication,
            String expectedRetryTopic,
            String expectedOperations
    ) throws Exception {
        Mockito.doReturn(maximumAttempts).when(this.webhookProperties).maximumAttempts();
        String consumerRoute = this.adviseDeliveryRoutes(topic);

        List<String> operations = new ArrayList<>();
        int replayCount = topic.equals("webhook-events") ? 0 : 2;
        Instant createdAt = Instant.EPOCH;
        ByteBuffer payloadByteBuffer = ByteBuffer.wrap(new byte[]{9, 0, -1, 10, 32, 9}, 1, 4);
        WebhookEvent webhookEvent = new WebhookEvent("event-1", "order.created", "tenant-1", "endpoint-1", createdAt, payloadByteBuffer, retryCount, nextEligibleAt, priorAttempts, replayCount);
        Map<String, String> details = deliveryOutcome == DeliveryOutcome.SUCCEEDED ? Map.of() : Map.of("type", "HTTP_FAILURE");
        IllegalStateException publicationException = new IllegalStateException("publication failed at " + failedPublication);
        if (failedPublication == FailedPublication.ATTEMPT_DETAILS) {
            Mockito.doThrow(publicationException).when(this.jsonMapper).writeValueAsBytes(details);
        }
        Mockito.when(this.restComponent.sendWebhook(webhookEvent)).thenAnswer(_ -> {
            operations.add("HTTP");
            return new DeliveryAttemptResult(deliveryOutcome, httpStatus, 1L, Instant.EPOCH, Instant.EPOCH, details);
        });
        KafkaManualCommit kafkaManualCommit = Mockito.mock(KafkaManualCommit.class);
        Mockito.doAnswer(_ -> {
                    operations.add("commit");
                    return null;
                })
                .when(kafkaManualCommit)
                .commit();

        MockEndpoint attemptMockEndpoint = this.camelContext.getEndpoint(ATTEMPT_OUTPUT, MockEndpoint.class);
        attemptMockEndpoint.whenAnyExchangeReceived(_ -> {
            operations.add("attempt");
            if (failedPublication == FailedPublication.ATTEMPT) {
                throw publicationException;
            }
        });
        MockEndpoint retryMockEndpoint = this.camelContext.getEndpoint(RETRY_OUTPUT, MockEndpoint.class);
        retryMockEndpoint.whenAnyExchangeReceived(_ -> {
            operations.add("retry");
            if (failedPublication == FailedPublication.RETRY) {
                throw publicationException;
            }
        });
        MockEndpoint deadLetterMockEndpoint = this.camelContext.getEndpoint(DEAD_LETTER_OUTPUT, MockEndpoint.class);
        deadLetterMockEndpoint.whenAnyExchangeReceived(_ -> {
            operations.add("dead-letter");
            if (failedPublication == FailedPublication.DEAD_LETTER) {
                throw publicationException;
            }
        });
        this.startDeliveryRoutes(consumerRoute);

        Throwable publicationFailure = Assertions.catchThrowable(
                () -> this.producerTemplate.sendBodyAndHeaders(
                        INPUT,
                        webhookEvent,
                        Map.of(
                                KafkaConstants.MANUAL_COMMIT, kafkaManualCommit,
                                KafkaConstants.KEY, "wrong-inbound-key",
                                KafkaConstants.TOPIC, topic,
                                KafkaConstants.PARTITION, 11,
                                "source-metadata", "discard",
                                "webhook-signature", "discard"
                        )
                )
        );

        if (failedPublication == FailedPublication.NONE) {
            Assertions.assertThat(publicationFailure).isNull();
        } else if (failedPublication == FailedPublication.DEAD_LETTER) {
            Assertions.assertThat(publicationFailure).hasRootCauseMessage("Dead-letter publication failed [topic=webhook-events-dead-letter]");
        } else {
            Assertions.assertThat(publicationFailure).hasRootCause(publicationException);
        }
        Assertions.assertThat(operations).containsExactly(expectedOperations.split("/"));
        boolean terminal = expectedOperations.contains("/dead-letter");
        if (terminal) {
            WebhookEvent expectedWebhookEvent = new WebhookEvent(
                    "event-1",
                    "order.created",
                    "tenant-1",
                    "endpoint-1",
                    createdAt,
                    ByteBuffer.wrap(new byte[]{0, -1, 10, 32}),
                    retryCount,
                    nextEligibleAt,
                    priorAttempts + 1,
                    replayCount
            );
            Assertions.assertThat(deadLetterMockEndpoint.getExchanges()).singleElement().satisfies(exchange -> {
                Assertions.assertThat(exchange.getMessage().getBody()).isEqualTo(expectedWebhookEvent);
                Assertions.assertThat(exchange.getMessage().getHeaders()).containsExactlyEntriesOf(Map.of(KafkaConstants.KEY, "endpoint-1"));
            });
        } else {
            Assertions.assertThat(deadLetterMockEndpoint.getExchanges()).isEmpty();
        }
        if (failedPublication == FailedPublication.DEAD_LETTER || failedPublication == FailedPublication.ATTEMPT_DETAILS) {
            Assertions.assertThat(attemptMockEndpoint.getExchanges()).isEmpty();
        } else {
            Assertions.assertThat(attemptMockEndpoint.getExchanges()).singleElement().satisfies(exchange -> {
                DeliveryAttempt deliveryAttempt = exchange.getMessage().getBody(DeliveryAttempt.class);
                Assertions.assertThat(deliveryAttempt.getAttemptNumber()).isEqualTo(priorAttempts + 1);
                Assertions.assertThat(deliveryAttempt.getOutcome()).isEqualTo(gregor.avro.DeliveryOutcome.valueOf(deliveryOutcome.name()));
                Assertions.assertThat(deliveryAttempt.getHttpStatus()).isEqualTo(httpStatus);
                Assertions.assertThat(deliveryAttempt.getRetryCount()).isEqualTo(retryCount);
                Assertions.assertThat(deliveryAttempt.getReplayCount()).isEqualTo(replayCount);
                Assertions.assertThat(deliveryAttempt.getDeadLettered()).isEqualTo(terminal);
                Assertions.assertThat(exchange.getMessage().getHeaders()).containsExactlyEntriesOf(Map.of(KafkaConstants.KEY, "event-1"));
            });
        }
        if (expectedRetryTopic != null) {
            Assertions.assertThat(retryMockEndpoint.getExchanges()).singleElement().satisfies(exchange -> {
                WebhookEvent retryWebhookEvent = exchange.getMessage().getBody(WebhookEvent.class);
                Assertions.assertThat(retryWebhookEvent.getEventId()).isEqualTo("event-1");
                Assertions.assertThat(retryWebhookEvent.getDeliveryAttemptCount()).isEqualTo(priorAttempts + 1);
                Assertions.assertThat(retryWebhookEvent.getRetryCount()).isEqualTo(retryCount + 1);
                Assertions.assertThat(exchange.getMessage().getHeaders()).containsExactlyInAnyOrderEntriesOf(Map.of(KafkaConstants.KEY, "endpoint-1", KafkaConstants.OVERRIDE_TOPIC, expectedRetryTopic));
            });
        } else {
            Assertions.assertThat(retryMockEndpoint.getExchanges()).isEmpty();
        }
        Mockito.verify(this.restComponent).sendWebhook(webhookEvent);
    }

    @ParameterizedTest
    @CsvSource({
            "webhook-events, -1,",
            "webhook-events-retry-1m, 0,",
            "webhook-events-retry-1h, 4, 1970-01-01T00:01:00Z"
    })
    void commitsRejectedEnvelopesWithoutHttpOrPublication(String topic, int priorAttempts, Instant nextEligibleAt) throws Exception {
        String consumerRoute = this.adviseDeliveryRoutes(topic);
        KafkaManualCommit kafkaManualCommit = Mockito.mock(KafkaManualCommit.class);
        WebhookEvent webhookEvent = new WebhookEvent("event-1", "order.created", "tenant-1", "endpoint-1", Instant.EPOCH, ByteBuffer.wrap(new byte[]{1}), 0, nextEligibleAt, priorAttempts, 0);
        this.startDeliveryRoutes(consumerRoute);

        this.producerTemplate.sendBodyAndHeaders(INPUT, webhookEvent, Map.of(KafkaConstants.MANUAL_COMMIT, kafkaManualCommit, KafkaConstants.TOPIC, topic));

        Mockito.verify(kafkaManualCommit).commit();
        Mockito.verifyNoInteractions(this.restComponent);
        for (String uri : List.of(ATTEMPT_OUTPUT, RETRY_OUTPUT, DEAD_LETTER_OUTPUT)) {
            Assertions.assertThat(this.camelContext.getEndpoint(uri, MockEndpoint.class).getExchanges()).isEmpty();
        }
    }

    private String adviseDeliveryRoutes(String topic) throws Exception {
        String consumerRoute = "kafka:" + topic + "?groupId=" + topic;
        AdviceWith.adviceWith(this.camelContext, consumerRoute, adviceWithRouteBuilder -> adviceWithRouteBuilder.replaceFromWith(INPUT));
        AdviceWith.adviceWith(
                this.camelContext,
                WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT,
                adviceWithRouteBuilder -> {
                    adviceWithRouteBuilder.weaveByToUri("kafka:" + WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC).replace().to(ATTEMPT_OUTPUT);
                    adviceWithRouteBuilder.weaveByToUri("kafka:" + WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC).replace().to(RETRY_OUTPUT);
                    adviceWithRouteBuilder.interceptSendToEndpoint("kafka:webhook-events-dead-letter").skipSendToOriginalEndpoint().to(DEAD_LETTER_OUTPUT);
                }
        );
        return consumerRoute;
    }

    private void startDeliveryRoutes(String consumerRoute) throws Exception {
        this.camelContext.start();
        for (String routeId : List.of(WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT, WebhookRoute.WEBHOOK_DELIVERY_ATTEMPT_DIRECT, WebhookRoute.WEBHOOK_RETRY_ELIGIBILITY_DIRECT, consumerRoute)) {
            this.camelContext.getRouteController().startRoute(routeId);
        }
    }

    private enum FailedPublication {
        NONE, ATTEMPT_DETAILS, ATTEMPT, RETRY, DEAD_LETTER
    }
}
