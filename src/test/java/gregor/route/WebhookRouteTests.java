package gregor.route;

import gregor.avro.DeliveryAttempt;
import gregor.avro.WebhookEvent;
import gregor.config.properties.WebhookProperties;
import gregor.dto.DeliveryAttemptResult;
import gregor.dto.DeliveryOutcome;
import gregor.dto.WebhookRouteMessage;
import gregor.exception.EligibilityWaitInterruptedException;
import gregor.service.WebhookService;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.model.ModelCamelContext;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.model.RouteDefinitionHelper;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.apache.camel.test.spring.junit5.UseAdviceWith;
import org.apache.camel.util.URISupport;
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
import java.util.Set;
import java.util.stream.Collectors;

@UseAdviceWith
@SpringBootTest(properties = {
        "camel.component.kafka.max-poll-interval-ms=4500000",
        "gregor.kafka.topics.retry-retention=7d",
        "gregor.kafka.topics.dead-letter-retention=30d"
})
@CamelSpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class WebhookRouteTests {

    private static final String WEBHOOK_DELIVERY_INPUT = "direct:webhook-delivery-in";
    private static final String WEBHOOK_RETRY_INPUT = "direct:webhook-retry-in";
    private static final String ATTEMPT_PERSISTENCE_INPUT = "direct:delivery-attempt-persistence-in";
    private static final String DELIVERY_ATTEMPT_OUTPUT = "mock:persist-delivery-attempt";
    private static final String WEBHOOK_DELIVERY_URI =
            "kafka:" + WebhookRoute.WEBHOOK_EVENT_TOPIC + "?groupId=" + WebhookRoute.WEBHOOK_EVENT_TOPIC;
    private static final String WEBHOOK_RETRY_1M_URI =
            "kafka:" + WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC
                    + "?groupId=" + WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC;
    private static final String WEBHOOK_RETRY_10M_URI =
            "kafka:" + WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC
                    + "?groupId=" + WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC;
    private static final String WEBHOOK_RETRY_1H_URI =
            "kafka:" + WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC
                    + "?groupId=" + WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC;
    private static final List<String> WEBHOOK_RETRY_URIS = List.of(
            WEBHOOK_RETRY_1M_URI,
            WEBHOOK_RETRY_10M_URI,
            WEBHOOK_RETRY_1H_URI
    );
    private static final String DELIVERY_ATTEMPT_PERSISTENCE_URI =
            "kafka:" + WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC
                    + "?groupId=" + WebhookRoute.PERSIST_DELIVERY_ATTEMPT_TOPIC;

    @Autowired
    private CamelContext camelContext;
    @Autowired
    private ProducerTemplate producerTemplate;
    @Autowired
    private WebhookProperties webhookProperties;
    @MockitoBean
    private WebhookService webhookService;

    @Test
    void declaresEightRoutesWithTopicNamedRetryConsumerGroupsAndNoRetryOrDeadLetterProducer() throws Exception {
        ModelCamelContext modelCamelContext = (ModelCamelContext) this.camelContext;

        Assertions.assertThat(sourceUri(modelCamelContext, WEBHOOK_DELIVERY_URI)).isEqualTo(WEBHOOK_DELIVERY_URI);
        for (String retryUri : WEBHOOK_RETRY_URIS) {
            Assertions.assertThat(sourceUri(modelCamelContext, retryUri)).isEqualTo(retryUri);
            Assertions.assertThat(outputUris(this.camelContext, modelCamelContext, retryUri))
                    .containsOnly(URISupport.normalizeUri(WebhookRoute.WEBHOOK_RETRY_ELIGIBILITY_DIRECT));
        }
        Assertions.assertThat(sourceUri(modelCamelContext, WebhookRoute.WEBHOOK_RETRY_ELIGIBILITY_DIRECT))
                .isEqualTo(WebhookRoute.WEBHOOK_RETRY_ELIGIBILITY_DIRECT);
        Assertions.assertThat(sourceUri(modelCamelContext, WebhookRoute.WEBHOOK_DELIVERY_ATTEMPT_DIRECT))
                .isEqualTo(WebhookRoute.WEBHOOK_DELIVERY_ATTEMPT_DIRECT);
        Assertions.assertThat(sourceUri(modelCamelContext, WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT))
                .isEqualTo(WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT);
        Assertions.assertThat(sourceUri(modelCamelContext, DELIVERY_ATTEMPT_PERSISTENCE_URI))
                .isEqualTo(DELIVERY_ATTEMPT_PERSISTENCE_URI);
        Assertions.assertThat(modelCamelContext.getRouteDefinitions()).hasSize(8);
        Set<String> outputUris = modelCamelContext.getRouteDefinitions().stream()
                .flatMap(routeDefinition -> RouteDefinitionHelper.gatherAllStaticEndpointUris(
                        this.camelContext,
                        routeDefinition,
                        false,
                        true
                ).stream())
                .collect(Collectors.toSet());
        Assertions.assertThat(outputUris).doesNotContain(
                URISupport.normalizeUri("kafka:" + WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC),
                URISupport.normalizeUri("kafka:" + WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC),
                URISupport.normalizeUri("kafka:" + WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC),
                URISupport.normalizeUri("kafka:" + WebhookRoute.WEBHOOK_EVENT_DEAD_LETTER_TOPIC)
        );
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

        Instant futureEligibility = Instant.parse("2026-07-10T13:00:00Z");
        for (DeliveryOutcome outcome : List.of(DeliveryOutcome.SUCCEEDED, DeliveryOutcome.PERMANENT_FAILURE)) {
            WebhookEvent webhookEvent = event("event-" + outcome.name(), 0, 0, 0, futureEligibility);
            DeliveryAttemptResult deliveryAttemptResult = result(outcome);
            DeliveryAttempt attempt = attempt(webhookEvent.getEventId(), 1, outcome);
            WebhookRouteMessage initial = new WebhookRouteMessage(webhookEvent, null);
            WebhookRouteMessage sent = new WebhookRouteMessage(
                    event("event-" + outcome.name(), 0, 1, 0, futureEligibility),
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
            Mockito.verify(kafkaManualCommit).commit();
        }
        Mockito.verify(this.webhookService, Mockito.never()).hasMalformedRetryEnvelope(Mockito.any());
        Mockito.verify(this.webhookService, Mockito.never()).awaitEligibility(Mockito.any());
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
    void eligibleRetryRecordsUseTheSharedDeliveryPathAndCommitAfterEveryAttemptPublication() throws Exception {
        this.adviseRetryConsumerAndAttemptPublication(WEBHOOK_RETRY_1M_URI);
        List<String> operations = new ArrayList<>();
        MockEndpoint attempts = this.camelContext.getEndpoint(DELIVERY_ATTEMPT_OUTPUT, MockEndpoint.class);
        attempts.expectedMessageCount(2);
        attempts.whenAnyExchangeReceived(exchange -> operations.add(
                "publish-" + exchange.getMessage().getBody(DeliveryAttempt.class).getAttemptNumber()
        ));
        Instant nextEligibleAt = Instant.EPOCH;
        WebhookEvent webhookEvent = event("event-retry-tier", 2, 2, 3, nextEligibleAt);
        DeliveryAttemptResult deliveryAttemptResult = result(DeliveryOutcome.RETRYABLE_FAILURE);
        WebhookRouteMessage initialMessage = new WebhookRouteMessage(webhookEvent, null);
        WebhookRouteMessage thirdAttemptMessage = new WebhookRouteMessage(
                event("event-retry-tier", 2, 3, 3, nextEligibleAt),
                deliveryAttemptResult
        );
        WebhookRouteMessage fourthAttemptMessage = new WebhookRouteMessage(
                event("event-retry-tier", 2, 4, 3, nextEligibleAt),
                deliveryAttemptResult
        );
        Mockito.when(this.webhookService.createWebhookRouteMessage(webhookEvent)).thenReturn(initialMessage);
        Mockito.doAnswer(_ -> {
            operations.add("await");
            return null;
        }).when(this.webhookService).awaitEligibility(initialMessage);
        Mockito.when(this.webhookService.sendWebhookEvent(initialMessage)).thenAnswer(_ -> {
            operations.add("send-3");
            return thirdAttemptMessage;
        });
        Mockito.when(this.webhookService.sendWebhookEvent(thirdAttemptMessage)).thenAnswer(_ -> {
            operations.add("send-4");
            return fourthAttemptMessage;
        });
        Mockito.when(this.webhookService.createAttempt(thirdAttemptMessage))
                .thenReturn(attempt("event-retry-tier", 3, DeliveryOutcome.RETRYABLE_FAILURE, 2, 3));
        Mockito.when(this.webhookService.createAttempt(fourthAttemptMessage))
                .thenReturn(attempt("event-retry-tier", 4, DeliveryOutcome.RETRYABLE_FAILURE, 2, 3));
        Mockito.when(this.webhookService.shouldRetry(thirdAttemptMessage)).thenReturn(true);
        Mockito.when(this.webhookService.shouldRetry(fourthAttemptMessage)).thenReturn(false);
        KafkaManualCommit kafkaManualCommit = Mockito.mock(KafkaManualCommit.class);
        Mockito.doAnswer(_ -> {
            operations.add("commit");
            return null;
        }).when(kafkaManualCommit).commit();
        this.startRoutes(
                WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT,
                WebhookRoute.WEBHOOK_DELIVERY_ATTEMPT_DIRECT,
                WebhookRoute.WEBHOOK_RETRY_ELIGIBILITY_DIRECT,
                WEBHOOK_RETRY_1M_URI
        );

        this.producerTemplate.sendBodyAndHeaders(
                WEBHOOK_RETRY_INPUT,
                webhookEvent,
                Map.of(
                        KafkaConstants.MANUAL_COMMIT, kafkaManualCommit,
                        KafkaConstants.TOPIC, WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC
                )
        );

        MockEndpoint.assertIsSatisfied(this.camelContext);
        Assertions.assertThat(operations).containsExactly(
                "await",
                "send-3",
                "publish-3",
                "send-4",
                "publish-4",
                "commit"
        );
        Assertions.assertThat(attempts.getExchanges())
                .extracting(exchange -> exchange.getMessage().getBody(DeliveryAttempt.class).getRetryCount())
                .containsOnly(2);
        Assertions.assertThat(attempts.getExchanges())
                .extracting(exchange -> exchange.getMessage().getBody(DeliveryAttempt.class).getReplayCount())
                .containsOnly(3);
        Mockito.verify(this.webhookService).hasMalformedRetryEnvelope(initialMessage);
        Mockito.verify(this.webhookService).awaitEligibility(initialMessage);
        Mockito.verify(this.webhookService).sendWebhookEvent(initialMessage);
        Mockito.verify(this.webhookService).sendWebhookEvent(thirdAttemptMessage);
        Mockito.verify(this.webhookService, Mockito.times(2)).hasImpossibleDeliveryState(Mockito.any());
        Mockito.verify(kafkaManualCommit).commit();
    }

    @Test
    void malformedRetryEnvelopeCommitsWithoutHttpPublicationOrPersistence() throws Exception {
        this.adviseRetryConsumerAndAttemptPublication(WEBHOOK_RETRY_10M_URI);
        MockEndpoint attempts = this.camelContext.getEndpoint(DELIVERY_ATTEMPT_OUTPUT, MockEndpoint.class);
        attempts.expectedMessageCount(0);
        WebhookEvent webhookEvent = event("event-malformed-retry");
        WebhookRouteMessage initialMessage = new WebhookRouteMessage(webhookEvent, null);
        Mockito.when(this.webhookService.createWebhookRouteMessage(webhookEvent)).thenReturn(initialMessage);
        Mockito.when(this.webhookService.hasMalformedRetryEnvelope(initialMessage)).thenReturn(true);
        KafkaManualCommit kafkaManualCommit = Mockito.mock(KafkaManualCommit.class);
        this.startRoutes(
                WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT,
                WebhookRoute.WEBHOOK_DELIVERY_ATTEMPT_DIRECT,
                WebhookRoute.WEBHOOK_RETRY_ELIGIBILITY_DIRECT,
                WEBHOOK_RETRY_10M_URI
        );

        Assertions.assertThatCode(() -> this.producerTemplate.sendBodyAndHeaders(
                WEBHOOK_RETRY_INPUT,
                webhookEvent,
                Map.of(
                        KafkaConstants.MANUAL_COMMIT, kafkaManualCommit,
                        KafkaConstants.TOPIC, WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC
                )
        )).doesNotThrowAnyException();

        MockEndpoint.assertIsSatisfied(this.camelContext);
        Mockito.verify(this.webhookService).hasMalformedRetryEnvelope(initialMessage);
        Mockito.verify(this.webhookService).logDiscardedRetryEnvelope(
                initialMessage,
                WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC
        );
        Mockito.verify(this.webhookService, Mockito.never()).awaitEligibility(Mockito.any());
        Mockito.verify(this.webhookService, Mockito.never()).sendWebhookEvent(Mockito.any());
        Mockito.verify(this.webhookService, Mockito.never()).createAttempt(Mockito.any());
        Mockito.verify(this.webhookService, Mockito.never()).persistDeliveryAttempt(Mockito.any());
        Mockito.verify(kafkaManualCommit).commit();
    }

    @Test
    void interruptedEligibilityWaitingPreventsRetryTopicCommit() throws Exception {
        this.adviseRetryConsumerAndAttemptPublication(WEBHOOK_RETRY_1H_URI);
        WebhookEvent webhookEvent = event("event-interrupted", 3, 1, 0, Instant.MAX);
        WebhookRouteMessage initialMessage = new WebhookRouteMessage(webhookEvent, null);
        Mockito.when(this.webhookService.createWebhookRouteMessage(webhookEvent)).thenReturn(initialMessage);
        Mockito.doThrow(new EligibilityWaitInterruptedException(new InterruptedException()))
                .when(this.webhookService)
                .awaitEligibility(initialMessage);
        KafkaManualCommit kafkaManualCommit = Mockito.mock(KafkaManualCommit.class);
        this.startRoutes(
                WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT,
                WebhookRoute.WEBHOOK_DELIVERY_ATTEMPT_DIRECT,
                WebhookRoute.WEBHOOK_RETRY_ELIGIBILITY_DIRECT,
                WEBHOOK_RETRY_1H_URI
        );

        Assertions.assertThatThrownBy(() -> this.producerTemplate.sendBodyAndHeaders(
                WEBHOOK_RETRY_INPUT,
                webhookEvent,
                Map.of(
                        KafkaConstants.MANUAL_COMMIT, kafkaManualCommit,
                        KafkaConstants.TOPIC, WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC
                )
        )).isInstanceOf(RuntimeException.class);

        Mockito.verify(this.webhookService).awaitEligibility(initialMessage);
        Mockito.verify(this.webhookService, Mockito.never()).sendWebhookEvent(Mockito.any());
        Mockito.verify(this.webhookService, Mockito.never()).createAttempt(Mockito.any());
        Mockito.verify(kafkaManualCommit, Mockito.never()).commit();
    }

    @Test
    void retryAttemptPublicationFailurePreventsRetryTopicCommit() throws Exception {
        this.adviseRetryConsumerAndAttemptPublication(WEBHOOK_RETRY_1M_URI);
        MockEndpoint attempts = this.camelContext.getEndpoint(DELIVERY_ATTEMPT_OUTPUT, MockEndpoint.class);
        attempts.whenAnyExchangeReceived(_ -> {
            throw new IllegalStateException("attempt publication failed");
        });
        WebhookEvent webhookEvent = event("event-retry-publication-failure", 1, 0, 0, Instant.EPOCH);
        DeliveryAttemptResult deliveryAttemptResult = result(DeliveryOutcome.RETRYABLE_FAILURE);
        DeliveryAttempt deliveryAttempt =
                attempt(webhookEvent.getEventId(), 1, deliveryAttemptResult.outcome(), 1, 0);
        WebhookRouteMessage initialMessage = new WebhookRouteMessage(webhookEvent, null);
        WebhookRouteMessage sentMessage = new WebhookRouteMessage(
                event("event-retry-publication-failure", 1, 1, 0, Instant.EPOCH),
                deliveryAttemptResult
        );
        Mockito.when(this.webhookService.createWebhookRouteMessage(webhookEvent)).thenReturn(initialMessage);
        Mockito.when(this.webhookService.sendWebhookEvent(initialMessage)).thenReturn(sentMessage);
        Mockito.when(this.webhookService.createAttempt(sentMessage)).thenReturn(deliveryAttempt);
        KafkaManualCommit kafkaManualCommit = Mockito.mock(KafkaManualCommit.class);
        this.startRoutes(
                WebhookRoute.WEBHOOK_DELIVERY_RESULT_DIRECT,
                WebhookRoute.WEBHOOK_DELIVERY_ATTEMPT_DIRECT,
                WebhookRoute.WEBHOOK_RETRY_ELIGIBILITY_DIRECT,
                WEBHOOK_RETRY_1M_URI
        );

        Assertions.assertThatThrownBy(() -> this.producerTemplate.sendBodyAndHeaders(
                WEBHOOK_RETRY_INPUT,
                webhookEvent,
                Map.of(
                        KafkaConstants.MANUAL_COMMIT, kafkaManualCommit,
                        KafkaConstants.TOPIC, WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC
                )
        )).isInstanceOf(RuntimeException.class);

        Mockito.verify(this.webhookService).awaitEligibility(initialMessage);
        Mockito.verify(this.webhookService).sendWebhookEvent(initialMessage);
        Mockito.verify(this.webhookService).createAttempt(sentMessage);
        Mockito.verify(this.webhookService, Mockito.never()).shouldRetry(Mockito.any());
        Mockito.verify(kafkaManualCommit, Mockito.never()).commit();
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
        this.adviseAttemptPublication();
    }

    private void adviseRetryConsumer(String retryConsumerUri) throws Exception {
        AdviceWith.adviceWith(
                this.camelContext,
                retryConsumerUri,
                advice -> advice.replaceFromWith(WEBHOOK_RETRY_INPUT)
        );
    }

    private void adviseRetryConsumerAndAttemptPublication(String retryConsumerUri) throws Exception {
        this.adviseRetryConsumer(retryConsumerUri);
        this.adviseAttemptPublication();
    }

    private void adviseAttemptPublication() throws Exception {
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

    private static Set<String> outputUris(
            CamelContext camelContext,
            ModelCamelContext modelCamelContext,
            String routeId
    ) {
        RouteDefinition definition = modelCamelContext.getRouteDefinition(routeId);
        Assertions.assertThat(definition).isNotNull();
        return RouteDefinitionHelper.gatherAllStaticEndpointUris(
                camelContext,
                definition,
                false,
                true
        );
    }

    private static WebhookEvent event(String eventId) {
        return event(eventId, 0);
    }

    private static WebhookEvent event(String eventId, int retryCount) {
        return event(eventId, retryCount, 0);
    }

    private static WebhookEvent event(String eventId, int retryCount, int deliveryAttemptCount) {
        return event(eventId, retryCount, deliveryAttemptCount, 0, null);
    }

    private static WebhookEvent event(
            String eventId,
            int retryCount,
            int deliveryAttemptCount,
            int replayCount,
            Instant nextEligibleAt
    ) {
        return new WebhookEvent(
                eventId,
                "order.created",
                "tenant-1",
                "endpoint-1",
                Instant.EPOCH,
                ByteBuffer.wrap("{}".getBytes(StandardCharsets.UTF_8)),
                retryCount,
                nextEligibleAt,
                deliveryAttemptCount,
                replayCount
        );
    }

    private static DeliveryAttemptResult result(DeliveryOutcome outcome) {
        return new DeliveryAttemptResult(outcome, 204, 1L, Instant.EPOCH, Instant.EPOCH.plusSeconds(2), Map.of());
    }

    private static DeliveryAttempt attempt(String eventId, int attemptNumber, DeliveryOutcome outcome) {
        return attempt(eventId, attemptNumber, outcome, 0, 0);
    }

    private static DeliveryAttempt attempt(
            String eventId,
            int attemptNumber,
            DeliveryOutcome outcome,
            int retryCount,
            int replayCount
    ) {
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
                retryCount,
                replayCount,
                false
        );
    }
}
