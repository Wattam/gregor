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
            """
            webhook-events, RETRYABLE_FAILURE, 0, 0, NONE, webhook-events-retry-1m, HTTP/attempt/retry/commit
            webhook-events-retry-1m, RETRYABLE_FAILURE, 1, 2, NONE, webhook-events-retry-1h, HTTP/attempt/retry/commit
            webhook-events, RETRYABLE_FAILURE, 0, 0, ATTEMPT, , HTTP/attempt
            webhook-events-retry-1m, RETRYABLE_FAILURE, 1, 2, ATTEMPT, , HTTP/attempt
            webhook-events, RETRYABLE_FAILURE, 0, 0, RETRY, webhook-events-retry-1m, HTTP/attempt/retry
            webhook-events-retry-1m, RETRYABLE_FAILURE, 1, 2, RETRY, webhook-events-retry-1h, HTTP/attempt/retry
            webhook-events, SUCCEEDED, 0, 0, NONE, , HTTP/attempt/commit
            webhook-events, PERMANENT_FAILURE, 0, 0, NONE, , HTTP/attempt/commit
            webhook-events-retry-1m, RETRYABLE_FAILURE, 3, 1, NONE, , HTTP/attempt/commit
            webhook-events-retry-1m, RETRYABLE_FAILURE, 1, 7, NONE, , HTTP/attempt/commit
            """
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
        AdviceWith.adviceWith(this.camelContext, consumerRoute, advice -> advice.replaceFromWith(INPUT));
        AdviceWith.adviceWith(this.camelContext, WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT, advice -> {
            advice.weaveByToUri("kafka:" + WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC).replace().to(ATTEMPT_OUTPUT);
            advice.weaveByToUri("kafka:" + WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC).replace().to(RETRY_OUTPUT);
            advice.interceptSendToEndpoint("kafka:" + WebhookRoute.WEBHOOK_EVENT_DEAD_LETTER_TOPIC + "*")
                    .skipSendToOriginalEndpoint().to("mock:dead-letter");
        });

        List<String> operations = new ArrayList<>();
        WebhookEvent webhookEvent = new WebhookEvent(
                "event-1",
                "order.created",
                "tenant-1",
                "endpoint-1",
                Instant.EPOCH,
                ByteBuffer.wrap(new byte[]{'{', '}'}),
                retryCount,
                Instant.EPOCH,
                priorAttempts,
                3
        );
        int status = switch (deliveryOutcome) {
            case RETRYABLE_FAILURE -> 503;
            case PERMANENT_FAILURE -> 400;
            case SUCCEEDED -> 204;
        };
        Mockito.when(this.restComponent.sendWebhook(webhookEvent)).thenAnswer(_ -> {
            operations.add("HTTP");
            return new DeliveryAttemptResult(deliveryOutcome, status, 1L, Instant.EPOCH, Instant.EPOCH, Map.of());
        });
        KafkaManualCommit manualCommit = Mockito.mock(KafkaManualCommit.class);
        Mockito.doAnswer(_ -> {
            operations.add("commit");
            return null;
        }).when(manualCommit).commit();

        IllegalStateException failure = new IllegalStateException("producer acknowledgement failed");
        MockEndpoint attempts = this.camelContext.getEndpoint(ATTEMPT_OUTPUT, MockEndpoint.class);
        attempts.whenAnyExchangeReceived(_ -> {
            operations.add("attempt");
            if (failedPublication == FailedPublication.ATTEMPT) {
                throw failure;
            }
        });
        MockEndpoint retries = this.camelContext.getEndpoint(RETRY_OUTPUT, MockEndpoint.class);
        retries.whenAnyExchangeReceived(_ -> {
            operations.add("retry");
            if (failedPublication == FailedPublication.RETRY) {
                throw failure;
            }
        });
        MockEndpoint deadLetters = this.camelContext.getEndpoint("mock:dead-letter", MockEndpoint.class);
        this.camelContext.start();
        for (String routeId : List.of(
                WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT,
                WebhookRoute.WEBHOOK_DELIVERY_ATTEMPT_DIRECT,
                WebhookRoute.WEBHOOK_RETRY_ELIGIBILITY_DIRECT,
                consumerRoute
        )) {
            this.camelContext.getRouteController().startRoute(routeId);
        }

        Throwable thrown = Assertions.catchThrowable(() -> this.producerTemplate.sendBodyAndHeaders(
                INPUT,
                webhookEvent,
                Map.of(
                        KafkaConstants.MANUAL_COMMIT, manualCommit,
                        KafkaConstants.KEY, "wrong-inbound-key",
                        KafkaConstants.TOPIC, topic,
                        KafkaConstants.PARTITION, 11,
                        "source-metadata", "discard"
                )
        ));

        if (failedPublication == FailedPublication.NONE) {
            Assertions.assertThat(thrown).isNull();
        } else {
            Assertions.assertThat(thrown).hasRootCause(failure);
        }
        Assertions.assertThat(operations).containsExactly(expectedOperations.split("/"));
        Assertions.assertThat(attempts.getExchanges()).singleElement().satisfies(exchange -> {
            DeliveryAttempt deliveryAttempt = exchange.getMessage().getBody(DeliveryAttempt.class);
            Assertions.assertThat(deliveryAttempt.getAttemptNumber()).isEqualTo(priorAttempts + 1);
            Assertions.assertThat(deliveryAttempt.getRetryCount()).isEqualTo(retryCount);
            Assertions.assertThat(deliveryAttempt.getReplayCount()).isEqualTo(3);
            Assertions.assertThat(deliveryAttempt.getDeadLettered()).isFalse();
            Assertions.assertThat(exchange.getMessage().getHeaders()).containsExactlyEntriesOf(
                    Map.of(KafkaConstants.KEY, "event-1")
            );
        });
        if (expectedRetryTopic != null) {
            Assertions.assertThat(retries.getExchanges()).singleElement().satisfies(exchange -> {
                WebhookEvent retry = exchange.getMessage().getBody(WebhookEvent.class);
                Assertions.assertThat(retry.getEventId()).isEqualTo("event-1");
                Assertions.assertThat(retry.getDeliveryAttemptCount()).isEqualTo(priorAttempts + 1);
                Assertions.assertThat(retry.getRetryCount()).isEqualTo(retryCount + 1);
                Assertions.assertThat(exchange.getMessage().getHeaders()).containsExactlyInAnyOrderEntriesOf(
                        Map.of(KafkaConstants.KEY, "endpoint-1", KafkaConstants.OVERRIDE_TOPIC, expectedRetryTopic)
                );
            });
        }
        Assertions.assertThat(deadLetters.getReceivedCounter()).isZero();
        Mockito.verify(this.restComponent).sendWebhook(webhookEvent);
    }

    private enum FailedPublication {
        NONE, ATTEMPT, RETRY
    }
}
