package gregor.route;

import gregor.avro.DeliveryAttempt;
import gregor.avro.WebhookEvent;
import gregor.dto.DeliveryAttemptResult;
import gregor.dto.DeliveryOutcome;
import gregor.dto.WebhookRouteMessage;
import gregor.service.WebhookService;
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
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@UseAdviceWith
@CamelSpringBootTest
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class WebhookRouteTests {

    private static final String INPUT = "direct:delivery-input";
    private static final String ATTEMPT_OUTPUT = "mock:delivery-attempt";

    @Autowired
    private CamelContext camelContext;
    @Autowired
    private ProducerTemplate producerTemplate;
    @MockitoBean
    private WebhookService webhookService;

    @ParameterizedTest
    @ValueSource(strings = {WebhookRoute.WEBHOOK_EVENT_TOPIC, WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC})
    void publishesEachAttemptBeforeRetryingOrCommitting(String topic) throws Exception {
        String consumerRoute = "kafka:" + topic + "?groupId=" + topic;
        boolean retryTier = !topic.equals(WebhookRoute.WEBHOOK_EVENT_TOPIC);
        AdviceWith.adviceWith(this.camelContext, consumerRoute, advice -> advice.replaceFromWith(INPUT));
        AdviceWith.adviceWith(
                this.camelContext,
                WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT,
                advice -> advice.weaveByToUri("kafka:" + WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC)
                        .replace().to(ATTEMPT_OUTPUT)
        );

        List<String> operations = new ArrayList<>();
        WebhookEvent event = new WebhookEvent(
                "event-1", "order.created", "tenant-1", "endpoint-1", Instant.EPOCH,
                ByteBuffer.wrap("{}".getBytes(StandardCharsets.UTF_8)), 0, Instant.EPOCH, 0, 0
        );
        WebhookRouteMessage initial = new WebhookRouteMessage(event, null);
        WebhookRouteMessage first = stubAttempt(initial, 1, operations);
        stubAttempt(first, 2, operations);
        Mockito.when(this.webhookService.createWebhookRouteMessage(event)).thenReturn(initial);
        Mockito.when(this.webhookService.shouldRetry(first)).thenReturn(true);
        Mockito.doAnswer(_ -> {
            operations.add("await");
            return null;
        }).when(this.webhookService).awaitEligibility(Mockito.any());
        KafkaManualCommit manualCommit = Mockito.mock(KafkaManualCommit.class);
        Mockito.doAnswer(_ -> {
            operations.add("commit");
            return null;
        }).when(manualCommit).commit();

        MockEndpoint attempts = this.camelContext.getEndpoint(ATTEMPT_OUTPUT, MockEndpoint.class);
        attempts.whenAnyExchangeReceived(exchange -> {
            operations.add("publish-" + exchange.getMessage().getBody(DeliveryAttempt.class).getAttemptNumber());
            Assertions.assertThat(exchange.getMessage().getHeaders())
                    .containsEntry(KafkaConstants.KEY, "event-1")
                    .doesNotContainKeys(KafkaConstants.MANUAL_COMMIT, "source-metadata");
        });
        this.camelContext.start();
        for (String routeId : List.of(
                WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT,
                WebhookRoute.WEBHOOK_DELIVERY_ATTEMPT_DIRECT,
                WebhookRoute.WEBHOOK_RETRY_ELIGIBILITY_DIRECT,
                consumerRoute
        )) {
            this.camelContext.getRouteController().startRoute(routeId);
        }

        this.producerTemplate.sendBodyAndHeaders(
                INPUT, event, Map.of(KafkaConstants.MANUAL_COMMIT, manualCommit, "source-metadata", "discard")
        );

        List<String> expected = retryTier
                ? List.of("await", "send-1", "publish-1", "send-2", "publish-2", "commit")
                : List.of("send-1", "publish-1", "send-2", "publish-2", "commit");
        Assertions.assertThat(operations).containsExactlyElementsOf(expected);
    }

    private WebhookRouteMessage stubAttempt(WebhookRouteMessage input, int number, List<String> operations) {
        WebhookEvent event = WebhookEvent.newBuilder(input.webhookEvent()).setDeliveryAttemptCount(number).build();
        WebhookRouteMessage sent = new WebhookRouteMessage(event, new DeliveryAttemptResult(
                DeliveryOutcome.RETRYABLE_FAILURE, 503, 1L, Instant.EPOCH, Instant.EPOCH, Map.of()
        ));
        Mockito.when(this.webhookService.sendWebhookEvent(input)).thenAnswer(_ -> {
            operations.add("send-" + number);
            return sent;
        });
        DeliveryAttempt attempt = new DeliveryAttempt(
                "attempt-" + number, event.getEventId(), event.getTenantId(), event.getEndpointId(),
                event.getEventType(), number, gregor.avro.DeliveryOutcome.RETRYABLE_FAILURE,
                503, 1L, Instant.EPOCH, ByteBuffer.wrap("{}".getBytes(StandardCharsets.UTF_8)), 0, 0, false
        );
        Mockito.when(this.webhookService.createAttempt(sent)).thenReturn(attempt);
        return sent;
    }
}
