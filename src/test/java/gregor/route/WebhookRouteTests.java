package gregor.route;

import gregor.avro.DeliveryAttempt;
import gregor.avro.WebhookEvent;
import gregor.component.RestComponent;
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

    @Autowired
    private CamelContext camelContext;
    @Autowired
    private ProducerTemplate producerTemplate;
    @MockitoBean
    private RestComponent restComponent;

    @ParameterizedTest
    @CsvSource({
            "webhook-events, RETRYABLE_FAILURE, 0, 0, NONE, webhook-events-retry-1m, HTTP/attempt/retry/commit",
            "webhook-events-retry-1m, RETRYABLE_FAILURE, 1, 2, NONE, webhook-events-retry-1h, HTTP/attempt/retry/commit",
            "webhook-events, RETRYABLE_FAILURE, 0, 0, ATTEMPT, , HTTP/attempt",
            "webhook-events-retry-1m, RETRYABLE_FAILURE, 1, 2, ATTEMPT, , HTTP/attempt",
            "webhook-events, RETRYABLE_FAILURE, 0, 0, RETRY, webhook-events-retry-1m, HTTP/attempt/retry",
            "webhook-events-retry-1m, RETRYABLE_FAILURE, 1, 2, RETRY, webhook-events-retry-1h, HTTP/attempt/retry",
            "webhook-events, SUCCEEDED, 0, 0, NONE, , HTTP/attempt/commit",
            "webhook-events, PERMANENT_FAILURE, 0, 0, NONE, , HTTP/attempt/commit",
            "webhook-events-retry-1m, RETRYABLE_FAILURE, 3, 1, NONE, , HTTP/attempt/commit",
            "webhook-events-retry-1m, RETRYABLE_FAILURE, 1, 7, NONE, , HTTP/attempt/commit"
    })
    void publishesOnceInOrderAndCommitsOnlyAfterRequiredSends(
            String topic,
            DeliveryOutcome deliveryOutcome,
            int priorAttempts,
            int retryCount,
            FailedPublication failedPublication,
            String expectedRetryTopic,
            String expectedOperations
    ) throws Exception {
        String consumerRoute = "kafka:" + topic + "?groupId=" + topic;
        AdviceWith.adviceWith(this.camelContext, consumerRoute, adviceWithRouteBuilder -> adviceWithRouteBuilder.replaceFromWith(INPUT));
        AdviceWith.adviceWith(
                this.camelContext,
                WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT,
                adviceWithRouteBuilder -> {
                    adviceWithRouteBuilder.weaveByToUri("kafka:" + WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC).replace().to(ATTEMPT_OUTPUT);
                    adviceWithRouteBuilder.weaveByToUri("kafka:" + WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC).replace().to(RETRY_OUTPUT);
                    adviceWithRouteBuilder.interceptSendToEndpoint("kafka:" + WebhookRoute.WEBHOOK_EVENT_DEAD_LETTER_TOPIC + "*").skipSendToOriginalEndpoint().to("mock:dead-letter");
                }
        );

        List<String> operations = new ArrayList<>();
        WebhookEvent webhookEvent = new WebhookEvent("event-1", "order.created", "tenant-1", "endpoint-1", Instant.EPOCH, ByteBuffer.wrap(new byte[]{'{', '}'}), retryCount, Instant.EPOCH, priorAttempts, 3);
        int httpStatus = switch (deliveryOutcome) {
            case RETRYABLE_FAILURE -> 503;
            case PERMANENT_FAILURE -> 400;
            case SUCCEEDED -> 204;
        };
        Mockito.when(this.restComponent.sendWebhook(webhookEvent)).thenAnswer(_ -> {
            operations.add("HTTP");
            return new DeliveryAttemptResult(deliveryOutcome, httpStatus, 1L, Instant.EPOCH, Instant.EPOCH, Map.of());
        });
        KafkaManualCommit kafkaManualCommit = Mockito.mock(KafkaManualCommit.class);
        Mockito.doAnswer(_ -> {
                    operations.add("commit");
                    return null;
                })
                .when(kafkaManualCommit)
                .commit();

        IllegalStateException publicationException = new IllegalStateException("producer acknowledgement failed");
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
        MockEndpoint deadLetterMockEndpoint = this.camelContext.getEndpoint("mock:dead-letter", MockEndpoint.class);
        this.camelContext.start();
        for (String routeId : List.of(WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT, WebhookRoute.WEBHOOK_DELIVERY_ATTEMPT_DIRECT, WebhookRoute.WEBHOOK_RETRY_ELIGIBILITY_DIRECT, consumerRoute)) {
            this.camelContext.getRouteController().startRoute(routeId);
        }

        Throwable publicationFailure = Assertions.catchThrowable(
                () -> this.producerTemplate.sendBodyAndHeaders(
                        INPUT,
                        webhookEvent,
                        Map.of(KafkaConstants.MANUAL_COMMIT, kafkaManualCommit, KafkaConstants.KEY, "wrong-inbound-key", KafkaConstants.TOPIC, topic, KafkaConstants.PARTITION, 11, "source-metadata", "discard")
                )
        );

        if (failedPublication == FailedPublication.NONE) {
            Assertions.assertThat(publicationFailure).isNull();
        } else {
            Assertions.assertThat(publicationFailure).hasRootCause(publicationException);
        }
        Assertions.assertThat(operations).containsExactly(expectedOperations.split("/"));
        Assertions.assertThat(attemptMockEndpoint.getExchanges()).singleElement().satisfies(exchange -> {
            DeliveryAttempt deliveryAttempt = exchange.getMessage().getBody(DeliveryAttempt.class);
            Assertions.assertThat(deliveryAttempt.getAttemptNumber()).isEqualTo(priorAttempts + 1);
            Assertions.assertThat(deliveryAttempt.getRetryCount()).isEqualTo(retryCount);
            Assertions.assertThat(deliveryAttempt.getReplayCount()).isEqualTo(3);
            Assertions.assertThat(deliveryAttempt.getDeadLettered()).isFalse();
            Assertions.assertThat(exchange.getMessage().getHeaders()).containsExactlyEntriesOf(Map.of(KafkaConstants.KEY, "event-1"));
        });
        if (expectedRetryTopic != null) {
            Assertions.assertThat(retryMockEndpoint.getExchanges()).singleElement().satisfies(exchange -> {
                WebhookEvent retryWebhookEvent = exchange.getMessage().getBody(WebhookEvent.class);
                Assertions.assertThat(retryWebhookEvent.getEventId()).isEqualTo("event-1");
                Assertions.assertThat(retryWebhookEvent.getDeliveryAttemptCount()).isEqualTo(priorAttempts + 1);
                Assertions.assertThat(retryWebhookEvent.getRetryCount()).isEqualTo(retryCount + 1);
                Assertions.assertThat(exchange.getMessage().getHeaders()).containsExactlyInAnyOrderEntriesOf(Map.of(KafkaConstants.KEY, "endpoint-1", KafkaConstants.OVERRIDE_TOPIC, expectedRetryTopic));
            });
        }
        Assertions.assertThat(deadLetterMockEndpoint.getReceivedCounter()).isZero();
        Mockito.verify(this.restComponent).sendWebhook(webhookEvent);
    }

    private enum FailedPublication {
        NONE, ATTEMPT, RETRY
    }
}
