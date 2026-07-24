package gregor.route;

import gregor.avro.DeliveryAttempt;
import gregor.avro.WebhookEvent;
import gregor.config.properties.WebhookProperties;
import gregor.dto.DeliveryAttemptResult;
import gregor.dto.DeliveryOutcome;
import gregor.dto.WebhookRouteMessage;
import gregor.service.WebhookService;
import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.model.ModelCamelContext;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.apache.camel.test.spring.junit5.UseAdviceWith;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
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
@SpringBootTest(properties = {
        "gregor.kafka.topics.retry-retention=7d",
        "gregor.kafka.topics.dead-letter-retention=30d"
})
@CamelSpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class WebhookRouteTests {

    private static final String WEBHOOK_DELIVERY_INPUT = "direct:webhook-delivery-in";
    private static final String ATTEMPT_PERSISTENCE_INPUT = "direct:delivery-attempt-persistence-in";
    private static final String DELIVERY_ATTEMPT_OUTPUT = "mock:persist-delivery-attempt";
    private static final String WEBHOOK_DELIVERY_URI =
            "kafka:" + WebhookRoute.WEBHOOK_EVENT_TOPIC + "?groupId=" + WebhookRoute.WEBHOOK_EVENT_TOPIC;
    private static final String DELIVERY_ATTEMPT_PERSISTENCE_URI =
            "kafka:" + WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC +
                    "?groupId=" + WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC;

    @Autowired
    private CamelContext camelContext;
    @Autowired
    private ProducerTemplate producerTemplate;
    @Autowired
    private WebhookProperties webhookProperties;
    @MockitoBean
    private WebhookService webhookService;

    @Test
    void declaresEachSourceUriAsItsRouteId() {
        ModelCamelContext modelCamelContext = (ModelCamelContext) this.camelContext;

        Assertions.assertThat(sourceUri(modelCamelContext, WEBHOOK_DELIVERY_URI)).isEqualTo(WEBHOOK_DELIVERY_URI);
        Assertions.assertThat(sourceUri(modelCamelContext, WebhookRoute.WEBHOOK_DELIVERY_ATTEMPT_DIRECT))
                .isEqualTo(WebhookRoute.WEBHOOK_DELIVERY_ATTEMPT_DIRECT);
        Assertions.assertThat(sourceUri(modelCamelContext, WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT))
                .isEqualTo(WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT);
        Assertions.assertThat(sourceUri(modelCamelContext, DELIVERY_ATTEMPT_PERSISTENCE_URI))
                .isEqualTo(DELIVERY_ATTEMPT_PERSISTENCE_URI);
        Assertions.assertThat(modelCamelContext.getRouteDefinitions()).hasSize(4);
        List<String> inactiveTopics = List.of(
                WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC,
                WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC,
                WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC,
                WebhookRoute.WEBHOOK_EVENT_DEAD_LETTER_TOPIC
        );
        Assertions.assertThat(this.camelContext.getEndpoints())
                .extracting(Endpoint::getEndpointUri)
                .noneMatch(endpointUri -> inactiveTopics.stream().anyMatch(endpointUri::contains));
    }

    @Test
    void terminalResultsPublishOneAttemptAndCommitWithoutRepublishingTheEvent() throws Exception {
        this.adviseDeliveryAndAttemptPublication();
        this.startRoutes(
                WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT,
                WebhookRoute.WEBHOOK_DELIVERY_ATTEMPT_DIRECT,
                WEBHOOK_DELIVERY_URI
        );
        MockEndpoint attempts = this.camelContext.getEndpoint(DELIVERY_ATTEMPT_OUTPUT, MockEndpoint.class);
        attempts.expectedMessageCount(2);

        for (DeliveryOutcome outcome : List.of(DeliveryOutcome.SUCCEEDED, DeliveryOutcome.PERMANENT_FAILURE)) {
            WebhookEvent webhookEvent = event("event-" + outcome.name());
            DeliveryAttemptResult deliveryAttemptResult = result(outcome);
            DeliveryAttempt attempt = attempt(webhookEvent.getEventId(), 1, outcome);
            WebhookRouteMessage initial = new WebhookRouteMessage(webhookEvent, null);
            WebhookRouteMessage sent = new WebhookRouteMessage(
                    event("event-" + outcome.name(), 0, 1),
                    deliveryAttemptResult
            );
            Mockito.when(this.webhookService.createWebhookRouteMessage(webhookEvent)).thenReturn(initial);
            Mockito.when(this.webhookService.sendWebhookEvent(initial)).thenReturn(sent);
            Mockito.when(this.webhookService.createAttempt(sent)).thenReturn(attempt);
            Mockito.when(this.webhookService.shouldRetry(sent)).thenReturn(false);
            KafkaManualCommit kafkaManualCommit = Mockito.mock(KafkaManualCommit.class);

            this.producerTemplate.sendBodyAndHeaders(
                    WEBHOOK_DELIVERY_INPUT,
                    webhookEvent,
                    Map.of(KafkaConstants.MANUAL_COMMIT, kafkaManualCommit, "source-metadata", "must-not-propagate")
            );

            Mockito.verify(this.webhookService).hasImpossibleDeliveryState(initial);
            Mockito.verify(this.webhookService).sendWebhookEvent(initial);
            Mockito.verify(this.webhookService).createAttempt(sent);
            Mockito.verify(this.webhookService).shouldRetry(sent);
            Mockito.verify(this.webhookService, Mockito.never()).logDiscardedEnvelope(Mockito.any());
            Mockito.verify(kafkaManualCommit).commit();
        }
        MockEndpoint.assertIsSatisfied(this.camelContext);
        Assertions.assertThat(attempts.getExchanges())
                .extracting(exchange -> exchange.getMessage().getHeader(KafkaConstants.KEY))
                .containsExactly("event-SUCCEEDED", "event-PERMANENT_FAILURE");
        Assertions.assertThat(attempts.getExchanges()).allSatisfy(exchange ->
                Assertions.assertThat(exchange.getMessage().getHeaders()).doesNotContainKey("source-metadata")
        );
    }

    @Test
    void retryableResultsPublishConfiguredMaximumAttemptsBeforeCommittingTheSourceOffset() throws Exception {
        this.adviseDeliveryAndAttemptPublication();
        int maximumAttempts = this.webhookProperties.maximumAttempts();
        List<String> operations = new ArrayList<>();
        MockEndpoint attempts = this.camelContext.getEndpoint(DELIVERY_ATTEMPT_OUTPUT, MockEndpoint.class);
        attempts.expectedMessageCount(maximumAttempts);
        attempts.whenAnyExchangeReceived(exchange -> operations.add(
                "publish-" + exchange.getMessage().getBody(DeliveryAttempt.class).getAttemptNumber()
        ));
        WebhookEvent webhookEvent = event("event-retry", 7);
        DeliveryAttemptResult deliveryAttemptResult = result(DeliveryOutcome.RETRYABLE_FAILURE);
        WebhookRouteMessage initialMessage = new WebhookRouteMessage(webhookEvent, null);
        List<WebhookRouteMessage> sentMessages = new ArrayList<>();
        WebhookRouteMessage inputMessage = initialMessage;
        for (int attemptNumber = 1; attemptNumber <= maximumAttempts; attemptNumber++) {
            WebhookRouteMessage sent = new WebhookRouteMessage(
                    event("event-retry", 7, attemptNumber),
                    deliveryAttemptResult
            );
            sentMessages.add(sent);
            Mockito.when(this.webhookService.sendWebhookEvent(inputMessage)).thenAnswer(_ -> {
                operations.add("send-" + sent.webhookEvent().getDeliveryAttemptCount());
                return sent;
            });
            Mockito.when(this.webhookService.createAttempt(sent)).thenReturn(attempt(
                    webhookEvent.getEventId(),
                    attemptNumber,
                    deliveryAttemptResult.outcome()
            ));
            Mockito.when(this.webhookService.shouldRetry(sent)).thenReturn(attemptNumber < maximumAttempts);
            inputMessage = sent;
        }
        Mockito.when(this.webhookService.createWebhookRouteMessage(webhookEvent)).thenReturn(initialMessage);
        KafkaManualCommit kafkaManualCommit = Mockito.mock(KafkaManualCommit.class);
        Mockito.doAnswer(_ -> {
            operations.add("commit");
            return null;
        }).when(kafkaManualCommit).commit();
        this.startRoutes(
                WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT,
                WebhookRoute.WEBHOOK_DELIVERY_ATTEMPT_DIRECT,
                WEBHOOK_DELIVERY_URI
        );

        this.producerTemplate.sendBodyAndHeader(
                WEBHOOK_DELIVERY_INPUT,
                webhookEvent,
                KafkaConstants.MANUAL_COMMIT,
                kafkaManualCommit
        );

        MockEndpoint.assertIsSatisfied(this.camelContext);
        List<String> expectedOperations = new ArrayList<>();
        for (int attemptNumber = 1; attemptNumber <= maximumAttempts; attemptNumber++) {
            expectedOperations.add("send-" + attemptNumber);
            expectedOperations.add("publish-" + attemptNumber);
        }
        expectedOperations.add("commit");
        Assertions.assertThat(operations).containsExactlyElementsOf(expectedOperations);
        Mockito.verify(this.webhookService).sendWebhookEvent(initialMessage);
        for (WebhookRouteMessage sentMessage : sentMessages.subList(0, maximumAttempts - 1)) {
            Mockito.verify(this.webhookService).sendWebhookEvent(sentMessage);
        }
        Mockito.verify(this.webhookService, Mockito.times(maximumAttempts))
                .hasImpossibleDeliveryState(Mockito.any());
        Mockito.verify(kafkaManualCommit).commit();
        Assertions.assertThat(webhookEvent.getRetryCount()).isEqualTo(7);
        Assertions.assertThat(webhookEvent.getDeliveryAttemptCount()).isZero();
    }

    @Test
    void attemptPublicationFailurePreventsRetryAndSourceCommit() throws Exception {
        this.adviseDeliveryAndAttemptPublication();
        MockEndpoint attempts = this.camelContext.getEndpoint(DELIVERY_ATTEMPT_OUTPUT, MockEndpoint.class);
        attempts.whenAnyExchangeReceived(_ -> {
            throw new IllegalStateException("attempt publication failed");
        });
        WebhookEvent webhookEvent = event("event-failure");
        DeliveryAttemptResult deliveryAttemptResult = result(DeliveryOutcome.RETRYABLE_FAILURE);
        DeliveryAttempt deliveryAttempt = attempt(webhookEvent.getEventId(), 1, deliveryAttemptResult.outcome());
        WebhookRouteMessage initialMessage = new WebhookRouteMessage(webhookEvent, null);
        WebhookRouteMessage sentMessage = new WebhookRouteMessage(
                event("event-failure", 0, 1),
                deliveryAttemptResult
        );
        Mockito.when(this.webhookService.createWebhookRouteMessage(webhookEvent)).thenReturn(initialMessage);
        Mockito.when(this.webhookService.sendWebhookEvent(initialMessage)).thenReturn(sentMessage);
        Mockito.when(this.webhookService.createAttempt(sentMessage)).thenReturn(deliveryAttempt);
        KafkaManualCommit kafkaManualCommit = Mockito.mock(KafkaManualCommit.class);
        this.startRoutes(
                WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT,
                WebhookRoute.WEBHOOK_DELIVERY_ATTEMPT_DIRECT,
                WEBHOOK_DELIVERY_URI
        );

        Assertions.assertThatThrownBy(() -> this.producerTemplate.sendBodyAndHeader(
                WEBHOOK_DELIVERY_INPUT,
                webhookEvent,
                KafkaConstants.MANUAL_COMMIT,
                kafkaManualCommit
        )).isInstanceOf(RuntimeException.class);

        Mockito.verify(this.webhookService).sendWebhookEvent(initialMessage);
        Mockito.verify(this.webhookService).createAttempt(sentMessage);
        Mockito.verify(this.webhookService, Mockito.never()).shouldRetry(Mockito.any());
        Mockito.verify(kafkaManualCommit, Mockito.never()).commit();
    }

    @Test
    void impossibleDeliveryStateDiscardsTheEnvelopeWithoutDeliveringOrPublishing() throws Exception {
        this.adviseDeliveryAndAttemptPublication();
        MockEndpoint attempts = this.camelContext.getEndpoint(DELIVERY_ATTEMPT_OUTPUT, MockEndpoint.class);
        attempts.expectedMessageCount(0);
        WebhookEvent webhookEvent = event("event-impossible", 0, this.webhookProperties.maximumAttempts());
        WebhookRouteMessage initialMessage = new WebhookRouteMessage(webhookEvent, null);
        Mockito.when(this.webhookService.createWebhookRouteMessage(webhookEvent)).thenReturn(initialMessage);
        Mockito.when(this.webhookService.hasImpossibleDeliveryState(initialMessage)).thenReturn(true);
        KafkaManualCommit kafkaManualCommit = Mockito.mock(KafkaManualCommit.class);
        this.startRoutes(
                WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT,
                WebhookRoute.WEBHOOK_DELIVERY_ATTEMPT_DIRECT,
                WEBHOOK_DELIVERY_URI
        );

        Assertions.assertThatCode(() -> this.producerTemplate.sendBodyAndHeader(
                WEBHOOK_DELIVERY_INPUT,
                webhookEvent,
                KafkaConstants.MANUAL_COMMIT,
                kafkaManualCommit
        )).doesNotThrowAnyException();

        MockEndpoint.assertIsSatisfied(this.camelContext);
        Mockito.verify(this.webhookService).logDiscardedEnvelope(initialMessage);
        Mockito.verify(this.webhookService, Mockito.never()).sendWebhookEvent(Mockito.any());
        Mockito.verify(this.webhookService, Mockito.never()).createAttempt(Mockito.any());
        Mockito.verify(this.webhookService, Mockito.never()).shouldRetry(Mockito.any());
        Mockito.verify(kafkaManualCommit).commit();
    }

    @Test
    void checksTheGuardBeforeEachImmediateRetryRequest() throws Exception {
        this.adviseDeliveryAndAttemptPublication();
        MockEndpoint attempts = this.camelContext.getEndpoint(DELIVERY_ATTEMPT_OUTPUT, MockEndpoint.class);
        attempts.expectedMessageCount(1);
        WebhookEvent webhookEvent = event("event-reentry");
        DeliveryAttemptResult deliveryAttemptResult = result(DeliveryOutcome.RETRYABLE_FAILURE);
        WebhookRouteMessage initialMessage = new WebhookRouteMessage(webhookEvent, null);
        WebhookRouteMessage sentMessage = new WebhookRouteMessage(
                event("event-reentry", 0, 1),
                deliveryAttemptResult
        );
        Mockito.when(this.webhookService.createWebhookRouteMessage(webhookEvent)).thenReturn(initialMessage);
        Mockito.when(this.webhookService.hasImpossibleDeliveryState(initialMessage)).thenReturn(false);
        Mockito.when(this.webhookService.hasImpossibleDeliveryState(sentMessage)).thenReturn(true);
        Mockito.when(this.webhookService.sendWebhookEvent(initialMessage)).thenReturn(sentMessage);
        Mockito.when(this.webhookService.createAttempt(sentMessage))
                .thenReturn(attempt("event-reentry", 1, DeliveryOutcome.RETRYABLE_FAILURE));
        Mockito.when(this.webhookService.shouldRetry(sentMessage)).thenReturn(true);
        KafkaManualCommit kafkaManualCommit = Mockito.mock(KafkaManualCommit.class);
        this.startRoutes(
                WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT,
                WebhookRoute.WEBHOOK_DELIVERY_ATTEMPT_DIRECT,
                WEBHOOK_DELIVERY_URI
        );

        this.producerTemplate.sendBodyAndHeader(
                WEBHOOK_DELIVERY_INPUT,
                webhookEvent,
                KafkaConstants.MANUAL_COMMIT,
                kafkaManualCommit
        );

        MockEndpoint.assertIsSatisfied(this.camelContext);
        Mockito.verify(this.webhookService).sendWebhookEvent(initialMessage);
        Mockito.verify(this.webhookService, Mockito.never()).sendWebhookEvent(sentMessage);
        Mockito.verify(this.webhookService).createAttempt(sentMessage);
        Mockito.verify(this.webhookService).shouldRetry(sentMessage);
        Mockito.verify(this.webhookService).logDiscardedEnvelope(sentMessage);
        Mockito.verify(this.webhookService, Mockito.times(2)).hasImpossibleDeliveryState(Mockito.any());
        Mockito.verify(kafkaManualCommit).commit();
    }

    @Test
    void persistenceCompletesBeforeTheAttemptOffsetCommits() throws Exception {
        AdviceWith.adviceWith(
                this.camelContext,
                DELIVERY_ATTEMPT_PERSISTENCE_URI,
                advice -> advice.replaceFromWith(ATTEMPT_PERSISTENCE_INPUT)
        );
        List<String> operations = new ArrayList<>();
        DeliveryAttempt deliveryAttempt = attempt("event-persist", 1, DeliveryOutcome.SUCCEEDED);
        Mockito.doAnswer(_ -> {
            operations.add("persist");
            return null;
        }).when(this.webhookService).persistDeliveryAttempt(deliveryAttempt);
        KafkaManualCommit kafkaManualCommit = Mockito.mock(KafkaManualCommit.class);
        Mockito.doAnswer(_ -> {
            operations.add("commit");
            return null;
        }).when(kafkaManualCommit).commit();
        this.startRoutes(DELIVERY_ATTEMPT_PERSISTENCE_URI);

        this.producerTemplate.sendBodyAndHeader(
                ATTEMPT_PERSISTENCE_INPUT,
                deliveryAttempt,
                KafkaConstants.MANUAL_COMMIT,
                kafkaManualCommit
        );

        Assertions.assertThat(operations).containsExactly("persist", "commit");
    }

    @Test
    void persistenceFailurePreventsTheAttemptOffsetCommit() throws Exception {
        AdviceWith.adviceWith(
                this.camelContext,
                DELIVERY_ATTEMPT_PERSISTENCE_URI,
                advice -> advice.replaceFromWith(ATTEMPT_PERSISTENCE_INPUT)
        );
        DeliveryAttempt deliveryAttempt = attempt("event-persist-failure", 1, DeliveryOutcome.SUCCEEDED);
        KafkaManualCommit kafkaManualCommit = Mockito.mock(KafkaManualCommit.class);
        Mockito.doThrow(new IllegalStateException("database unavailable"))
                .when(this.webhookService)
                .persistDeliveryAttempt(deliveryAttempt);
        this.startRoutes(DELIVERY_ATTEMPT_PERSISTENCE_URI);

        Assertions.assertThatThrownBy(() -> this.producerTemplate.sendBodyAndHeader(
                ATTEMPT_PERSISTENCE_INPUT,
                deliveryAttempt,
                KafkaConstants.MANUAL_COMMIT,
                kafkaManualCommit
        )).isInstanceOf(RuntimeException.class);

        Mockito.verify(this.webhookService).persistDeliveryAttempt(deliveryAttempt);
        Mockito.verify(kafkaManualCommit, Mockito.never()).commit();
    }

    private void adviseDeliveryAndAttemptPublication() throws Exception {
        AdviceWith.adviceWith(
                this.camelContext,
                WEBHOOK_DELIVERY_URI,
                advice -> advice.replaceFromWith(WEBHOOK_DELIVERY_INPUT)
        );
        AdviceWith.adviceWith(
                this.camelContext,
                WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT,
                advice -> advice.weaveByToUri("kafka:persist-delivery-attempt").replace().to(DELIVERY_ATTEMPT_OUTPUT)
        );
    }

    private void startRoutes(String... routeIds) throws Exception {
        this.camelContext.start();
        for (String routeId : routeIds) {
            this.camelContext.getRouteController().startRoute(routeId);
        }
    }

    private static String sourceUri(ModelCamelContext context, String routeId) {
        RouteDefinition definition = context.getRouteDefinition(routeId);
        Assertions.assertThat(definition).isNotNull();
        return definition.getInput().getEndpointUri();
    }

    private static WebhookEvent event(String eventId) {
        return event(eventId, 0);
    }

    private static WebhookEvent event(String eventId, int retryCount) {
        return event(eventId, retryCount, 0);
    }

    private static WebhookEvent event(String eventId, int retryCount, int deliveryAttemptCount) {
        return new WebhookEvent(
                eventId,
                "order.created",
                "tenant-1",
                "endpoint-1",
                Instant.EPOCH,
                ByteBuffer.wrap("{}".getBytes(StandardCharsets.UTF_8)),
                retryCount,
                null,
                deliveryAttemptCount,
                0
        );
    }

    private static DeliveryAttemptResult result(DeliveryOutcome outcome) {
        return new DeliveryAttemptResult(outcome, 204, 1L, Instant.EPOCH, Instant.EPOCH.plusSeconds(2), Map.of());
    }

    private static DeliveryAttempt attempt(String eventId, int attemptNumber, DeliveryOutcome outcome) {
        return new DeliveryAttempt(
                "attempt-" + eventId + "-" + attemptNumber,
                eventId,
                "tenant-1",
                "endpoint-1",
                "order.created",
                attemptNumber,
                gregor.avro.DeliveryOutcome.valueOf(outcome.name()),
                204,
                1L,
                Instant.EPOCH,
                ByteBuffer.wrap("{}".getBytes(StandardCharsets.UTF_8)),
                0,
                0,
                false
        );
    }
}
