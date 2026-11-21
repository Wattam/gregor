package gregor.service;

import gregor.avro.DeliveryAttempt;
import gregor.avro.WebhookEvent;
import gregor.component.RestComponent;
import gregor.config.WebhookPropertiesFixtures;
import gregor.config.properties.KafkaConsumerProperties;
import gregor.config.properties.WebhookProperties;
import gregor.dto.DeliveryAttemptResult;
import gregor.dto.DeliveryOutcome;
import gregor.dto.EventIngestRequest;
import gregor.dto.RetryPublication;
import gregor.dto.WebhookRouteMessage;
import gregor.exception.EligibilityWaitInterruptedException;
import gregor.exception.EventPublicationException;
import gregor.repository.DeliveryAttemptRepository;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.kafka.KafkaConstants;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

class WebhookServiceTests {

    private static final KafkaConsumerProperties KAFKA_CONSUMER_PROPERTIES = new KafkaConsumerProperties(4_500_000);

    @Test
    void sendsTheWebhookAndAdvancesTheEnvelopeDeliveryAttemptCount() {
        RestComponent restComponent = Mockito.mock(RestComponent.class);
        WebhookService webhookService = new WebhookService(
                restComponent,
                Mockito.mock(DeliveryAttemptRepository.class),
                new ObjectMapper(),
                WebhookPropertiesFixtures.create(),
                KAFKA_CONSUMER_PROPERTIES,
                Mockito.mock(ProducerTemplate.class),
                Clock.systemUTC()
        );
        WebhookEvent webhookEvent = event(2, 1, 3);
        DeliveryAttemptResult deliveryAttemptResult = new DeliveryAttemptResult(DeliveryOutcome.SUCCEEDED, 204, 1L, Instant.EPOCH, Instant.EPOCH.plusSeconds(2), Map.of());
        WebhookRouteMessage inputWebhookRouteMessage = new WebhookRouteMessage(webhookEvent, null);
        Mockito.when(restComponent.sendWebhook(webhookEvent)).thenReturn(deliveryAttemptResult);

        WebhookRouteMessage outputWebhookRouteMessage = webhookService.sendWebhookEvent(inputWebhookRouteMessage);

        Assertions.assertThat(outputWebhookRouteMessage).isEqualTo(new WebhookRouteMessage(event(3, 1, 3), deliveryAttemptResult));
        Assertions.assertThat(webhookEvent.getDeliveryAttemptCount()).isEqualTo(2);
        Mockito.verify(restComponent).sendWebhook(webhookEvent);
    }

    @Test
    void createsACompleteAttemptWithoutSensitiveEventData() {
        ObjectMapper objectMapper = new ObjectMapper();
        WebhookService webhookService = service(objectMapper);
        WebhookEvent webhookEvent = event(2, 5, 3);
        DeliveryAttemptResult deliveryAttemptResult = new DeliveryAttemptResult(
                DeliveryOutcome.RETRYABLE_FAILURE,
                503,
                37L,
                Instant.parse("2026-07-10T12:00:00Z"),
                Instant.parse("2026-07-10T12:00:02Z"),
                Map.of("type", "HTTP_FAILURE")
        );

        WebhookRouteMessage inputWebhookRouteMessage = new WebhookRouteMessage(webhookEvent, deliveryAttemptResult);

        DeliveryAttempt deliveryAttempt = webhookService.createAttempt(inputWebhookRouteMessage);

        Assertions.assertThat(deliveryAttempt.getEventId()).isEqualTo("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        Assertions.assertThat(deliveryAttempt.getTenantId()).isEqualTo("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
        Assertions.assertThat(deliveryAttempt.getEndpointId()).isEqualTo("cccccccc-cccc-cccc-cccc-cccccccccccc");
        Assertions.assertThat(deliveryAttempt.getEventType()).isEqualTo("order.created");
        Assertions.assertThat(deliveryAttempt.getAttemptNumber()).isEqualTo(2);
        Assertions.assertThat(deliveryAttempt.getOutcome()).isEqualTo(gregor.avro.DeliveryOutcome.RETRYABLE_FAILURE);
        Assertions.assertThat(deliveryAttempt.getHttpStatus()).isEqualTo(503);
        Assertions.assertThat(deliveryAttempt.getLatencyMs()).isEqualTo(37L);
        Assertions.assertThat(deliveryAttempt.getAttemptedAt()).isEqualTo(Instant.parse("2026-07-10T12:00:00Z"));
        Assertions.assertThat(deliveryAttempt.getRetryCount()).isEqualTo(5);
        Assertions.assertThat(deliveryAttempt.getReplayCount()).isEqualTo(3);
        Assertions.assertThat(deliveryAttempt.getDeadLettered()).isTrue();
        JsonNode detailsJsonNode = objectMapper.readTree(bytes(deliveryAttempt.getDetails()));
        Assertions.assertThat(detailsJsonNode).isEqualTo(objectMapper.readTree("{\"type\":\"HTTP_FAILURE\"}"));
    }

    @Test
    void mapsEveryOutcomeWithUniqueAttemptIdsAndNullableHttpStatus() {
        WebhookService webhookService = service(new ObjectMapper());
        WebhookEvent webhookEvent = event();
        Map<DeliveryOutcome, gregor.avro.DeliveryOutcome> outcomes = Map.of(
                DeliveryOutcome.SUCCEEDED, gregor.avro.DeliveryOutcome.SUCCEEDED,
                DeliveryOutcome.RETRYABLE_FAILURE, gregor.avro.DeliveryOutcome.RETRYABLE_FAILURE,
                DeliveryOutcome.PERMANENT_FAILURE, gregor.avro.DeliveryOutcome.PERMANENT_FAILURE
        );

        List<DeliveryAttempt> deliveryAttempts = new ArrayList<>();
        outcomes.forEach((inputDeliveryOutcome, expectedDeliveryOutcome) -> {
            DeliveryAttempt deliveryAttempt = webhookService.createAttempt(new WebhookRouteMessage(webhookEvent, new DeliveryAttemptResult(inputDeliveryOutcome, null, 1L, Instant.EPOCH, Instant.EPOCH, Map.of())));
            Assertions.assertThat(deliveryAttempt.getOutcome()).as("mapping for %s", inputDeliveryOutcome).isEqualTo(expectedDeliveryOutcome);
            deliveryAttempts.add(deliveryAttempt);
        });

        Assertions.assertThat(deliveryAttempts).extracting(DeliveryAttempt::getAttemptId).doesNotHaveDuplicates();
        Assertions.assertThat(deliveryAttempts).extracting(DeliveryAttempt::getHttpStatus).containsOnlyNulls();
    }

    @Test
    void propagatesAttemptDetailsSerializationFailure() {
        ObjectMapper objectMapper = Mockito.mock(ObjectMapper.class);
        WebhookService webhookService = service(objectMapper);
        DeliveryAttemptResult deliveryAttemptResult = new DeliveryAttemptResult(DeliveryOutcome.SUCCEEDED, 204, 1L, Instant.EPOCH, Instant.EPOCH.plusSeconds(2), Map.of());
        IllegalStateException serializationException = new IllegalStateException("serialization failed");
        Mockito.when(objectMapper.writeValueAsBytes(deliveryAttemptResult.details())).thenThrow(serializationException);

        WebhookRouteMessage webhookRouteMessage = new WebhookRouteMessage(event(1), deliveryAttemptResult);

        Assertions.assertThatThrownBy(() -> webhookService.createAttempt(webhookRouteMessage)).isSameAs(serializationException);
    }

    @ParameterizedTest
    @CsvSource({
            "0, 1, webhook-events-retry-1m, 2026-07-10T12:01:02.123Z",
            "1, 2, webhook-events-retry-10m, 2026-07-10T12:10:02.123Z",
            "2, 3, webhook-events-retry-1h, 2026-07-10T13:00:02.123Z"
    })
    void plansEachTierFromHttpCompletionPreservingTheEnvelope(int retryCount, int successorCount, String topic, Instant eligibility) {
        RestComponent restComponent = Mockito.mock(RestComponent.class);
        DeliveryAttemptRepository deliveryAttemptRepository = Mockito.mock(DeliveryAttemptRepository.class);
        ProducerTemplate producerTemplate = Mockito.mock(ProducerTemplate.class);
        Clock clock = Mockito.mock(Clock.class);
        Mockito.when(clock.instant()).thenReturn(Instant.parse("2026-07-10T12:00:10Z"));
        WebhookService webhookService = new WebhookService(restComponent, deliveryAttemptRepository, new ObjectMapper(), WebhookPropertiesFixtures.create(4), KAFKA_CONSUMER_PROPERTIES, producerTemplate, clock);
        Instant previousEligibility = Instant.parse("2026-07-10T11:55:00Z");
        byte[] payloadBytes = {0, -1, 10, 32};
        WebhookEvent inputWebhookEvent = WebhookEvent.newBuilder(webhookEvent(2, retryCount, 5, previousEligibility)).setPayload(ByteBuffer.wrap(payloadBytes)).build();
        WebhookEvent expectedWebhookEvent = WebhookEvent.newBuilder(inputWebhookEvent).setRetryCount(successorCount).setNextEligibleAt(eligibility).build();

        Assertions.assertThat(webhookService.planRetry(new WebhookRouteMessage(inputWebhookEvent, retryableResult()))).contains(new RetryPublication(topic, expectedWebhookEvent));
        Assertions.assertThat(inputWebhookEvent.getRetryCount()).isEqualTo(retryCount);
        Assertions.assertThat(inputWebhookEvent.getNextEligibleAt()).isEqualTo(previousEligibility);
        Assertions.assertThat(bytes(inputWebhookEvent.getPayload())).isEqualTo(payloadBytes);
    }

    @ParameterizedTest
    @CsvSource({
            "TIMEOUT, 2, false",
            "CONNECTION_FAILURE, 2, false",
            "INVALID_RESPONSE, 2, false",
            "TIMEOUT, 1, true",
            "CONNECTION_FAILURE, 1, true",
            "INVALID_RESPONSE, 1, true"
    })
    void appliesTheAttemptBudgetToTransportFailuresWithoutAnHttpStatus(String failureType, int maximumAttempts, boolean terminal) {
        WebhookService webhookService = service(new ObjectMapper(), WebhookPropertiesFixtures.create(maximumAttempts));
        WebhookEvent inputWebhookEvent = event(1);
        DeliveryAttemptResult deliveryAttemptResult = new DeliveryAttemptResult(
                DeliveryOutcome.RETRYABLE_FAILURE,
                null,
                2_123L,
                Instant.parse("2026-07-10T12:00:00Z"),
                Instant.parse("2026-07-10T12:00:02.123456789Z"),
                Map.of("type", failureType)
        );
        WebhookEvent expectedWebhookEvent = webhookEvent(1, 1, 0, Instant.parse("2026-07-10T12:01:02.123Z"));

        WebhookRouteMessage webhookRouteMessage = new WebhookRouteMessage(inputWebhookEvent, deliveryAttemptResult);
        Assertions.assertThat(webhookService.requiresDeadLetter(webhookRouteMessage)).isEqualTo(terminal);
        if (terminal) {
            Assertions.assertThat(webhookService.planRetry(webhookRouteMessage)).isEmpty();
        } else {
            Assertions.assertThat(webhookService.planRetry(webhookRouteMessage)).contains(new RetryPublication("webhook-events-retry-1m", expectedWebhookEvent));
        }
        DeliveryAttempt deliveryAttempt = webhookService.createAttempt(webhookRouteMessage);
        Assertions.assertThat(deliveryAttempt.getDeadLettered()).isEqualTo(terminal);
        Assertions.assertThat(deliveryAttempt.getOutcome()).isEqualTo(gregor.avro.DeliveryOutcome.RETRYABLE_FAILURE);
        Assertions.assertThat(deliveryAttempt.getHttpStatus()).isNull();
    }

    @ParameterizedTest
    @CsvSource({
            "3, RETRYABLE_FAILURE, 503, 3, 0, true",
            "4, SUCCEEDED, 204, 2, 0, false",
            "4, SUCCEEDED, 204, 4, 3, false",
            "4, PERMANENT_FAILURE, 400, 1, 0, true",
            "4, PERMANENT_FAILURE, 400, 2, 1, true",
            "4, PERMANENT_FAILURE, 400, 3, 2, true",
            "4, PERMANENT_FAILURE, 400, 4, 3, true",
            "4, RETRYABLE_FAILURE, 503, 2, 3, true",
            "4, RETRYABLE_FAILURE, 503, 2, 7, true",
            "4, RETRYABLE_FAILURE, 503, 2, 2147483647, true"
    })
    void distinguishesSuccessFromFailuresWithoutASuccessor(int maximumAttempts, DeliveryOutcome deliveryOutcome, int httpStatus, int attempt, int retries, boolean terminal) {
        WebhookService webhookService = service(new ObjectMapper(), WebhookPropertiesFixtures.create(maximumAttempts));
        WebhookRouteMessage webhookRouteMessage = new WebhookRouteMessage(event(attempt, retries, 0), new DeliveryAttemptResult(deliveryOutcome, httpStatus, 1L, Instant.EPOCH, Instant.EPOCH, Map.of()));

        Assertions.assertThat(webhookService.planRetry(webhookRouteMessage)).isEmpty();
        Assertions.assertThat(webhookService.requiresDeadLetter(webhookRouteMessage)).isEqualTo(terminal);
    }

    @ParameterizedTest
    @CsvSource({
            "4, 0",
            "2, -1"
    })
    void plansNoRetryWithACountBeyondTheBudgetOrANegativeRetryCount(int attempt, int retries) {
        WebhookService webhookService = service(new ObjectMapper(), WebhookPropertiesFixtures.create(3));
        WebhookRouteMessage webhookRouteMessage = new WebhookRouteMessage(event(attempt, retries, 0), retryableResult());

        Assertions.assertThat(webhookService.planRetry(webhookRouteMessage)).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({
            "1, 1, 0, '', true",
            "2, 1/2, 0/1, webhook-events-retry-1m, false/true",
            "3, 1/2/3, 0/1/2, webhook-events-retry-1m/webhook-events-retry-10m, false/false/true",
            "4, 1/2/3/4, 0/1/2/3, webhook-events-retry-1m/webhook-events-retry-10m/webhook-events-retry-1h, false/false/false/true"
    })
    void flagsOnlyTheLastAttemptInEachConfiguredRetryChain(int maximum, String attemptNumbers, String retryCounts, String tiers, String deadLetterFlags) {
        RestComponent restComponent = Mockito.mock(RestComponent.class);
        Mockito.when(restComponent.sendWebhook(Mockito.any())).thenReturn(retryableResult());
        ObjectMapper objectMapper = new ObjectMapper();
        ProducerTemplate producerTemplate = Mockito.mock(ProducerTemplate.class);
        WebhookService webhookService = new WebhookService(
                restComponent,
                Mockito.mock(DeliveryAttemptRepository.class),
                objectMapper,
                WebhookPropertiesFixtures.create(maximum),
                KAFKA_CONSUMER_PROPERTIES,
                producerTemplate,
                Clock.systemUTC()
        );
        webhookService.ingestEvent(new EventIngestRequest("order.created", objectMapper.createObjectNode()));
        ArgumentCaptor<WebhookEvent> ingestedWebhookEventCaptor = ArgumentCaptor.forClass(WebhookEvent.class);
        Mockito.verify(producerTemplate).sendBodyAndHeader(Mockito.anyString(), ingestedWebhookEventCaptor.capture(), Mockito.anyString(), Mockito.any());
        WebhookEvent nextWebhookEvent = ingestedWebhookEventCaptor.getValue();
        List<Integer> recordedAttemptNumbers = new ArrayList<>();
        List<Integer> recordedRetryCounts = new ArrayList<>();
        List<Boolean> recordedDeadLetterFlags = new ArrayList<>();
        List<String> retryTopics = new ArrayList<>();
        for (int delivery = 0; delivery < 4 && Objects.nonNull(nextWebhookEvent); delivery++) {
            WebhookRouteMessage deliveredWebhookRouteMessage = webhookService.sendWebhookEvent(webhookService.createWebhookRouteMessage(nextWebhookEvent));
            DeliveryAttempt deliveryAttempt = webhookService.createAttempt(deliveredWebhookRouteMessage);
            recordedAttemptNumbers.add(deliveryAttempt.getAttemptNumber());
            recordedRetryCounts.add(deliveryAttempt.getRetryCount());
            recordedDeadLetterFlags.add(deliveryAttempt.getDeadLettered());
            RetryPublication retryPublication = webhookService.planRetry(deliveredWebhookRouteMessage).orElse(null);
            nextWebhookEvent = Objects.isNull(retryPublication) ? null : retryPublication.webhookEvent();
            if (Objects.nonNull(retryPublication)) {
                retryTopics.add(retryPublication.topic());
            }
        }

        Assertions.assertThat(recordedAttemptNumbers).containsExactlyElementsOf(numbers(attemptNumbers));
        Assertions.assertThat(recordedRetryCounts).containsExactlyElementsOf(numbers(retryCounts));
        Assertions.assertThat(recordedDeadLetterFlags).containsExactlyElementsOf(Arrays.stream(deadLetterFlags.split("/")).map(Boolean::valueOf).toList());
        Assertions.assertThat(String.join("/", retryTopics)).isEqualTo(tiers);
        Assertions.assertThat(nextWebhookEvent).isNull();
        Mockito.verify(restComponent, Mockito.times(maximum)).sendWebhook(Mockito.any());
    }

    private static List<Integer> numbers(String values) {
        return Arrays.stream(values.split("/")).map(Integer::valueOf).toList();
    }

    private static DeliveryAttemptResult retryableResult() {
        return new DeliveryAttemptResult(DeliveryOutcome.RETRYABLE_FAILURE, 503, 2_123L, Instant.parse("2026-07-10T12:00:00Z"), Instant.parse("2026-07-10T12:00:02.123456789Z"), Map.of());
    }

    @Test
    void detectsOnlyImpossibleDeliveryStates() {
        WebhookProperties webhookProperties = WebhookPropertiesFixtures.create(4);
        WebhookService webhookService = service(new ObjectMapper(), webhookProperties);
        Map<WebhookEvent, Boolean> cases = new LinkedHashMap<>();
        cases.put(event(0), false);
        cases.put(event(3), false);
        cases.put(event(4), true);
        cases.put(event(5), true);
        cases.put(event(-1), true);
        cases.put(event(0, -1, 0), true);
        cases.put(event(0, 0, -1), true);
        cases.put(event(1, 3, 0), false);
        cases.put(webhookEvent(0, 0, 0, Instant.parse("2026-07-10T12:00:00Z")), false);

        for (Map.Entry<WebhookEvent, Boolean> testCase : cases.entrySet()) {
            WebhookRouteMessage webhookRouteMessage = new WebhookRouteMessage(testCase.getKey(), null);

            Assertions.assertThat(webhookService.hasImpossibleDeliveryState(webhookRouteMessage)).isEqualTo(testCase.getValue());
        }
    }

    @Test
    void detectsMalformedRetryEnvelopesAtTheMaximumPermittedWaitBoundary() {
        Instant now = Instant.parse("2026-07-10T12:00:00Z");
        WebhookService webhookService = webhookService(new ObjectMapper(), WebhookPropertiesFixtures.create(4), Mockito.mock(ProducerTemplate.class), Clock.fixed(now, ZoneOffset.UTC));
        Duration maximumPermittedWait = Duration.ofMinutes(70);
        Map<WebhookEvent, Boolean> cases = new LinkedHashMap<>();
        cases.put(webhookEvent(0, 0, 0, null), true);
        cases.put(webhookEvent(4, 0, 0, now), true);
        cases.put(webhookEvent(-1, 0, 0, now), true);
        cases.put(webhookEvent(0, -1, 0, now), true);
        cases.put(webhookEvent(0, 0, -1, now), true);
        cases.put(webhookEvent(0, 0, 0, now.plus(maximumPermittedWait).plusNanos(1)), true);
        cases.put(webhookEvent(0, 0, 0, now.plus(maximumPermittedWait)), false);
        cases.put(webhookEvent(0, 3, 0, now), false);

        for (Map.Entry<WebhookEvent, Boolean> testCase : cases.entrySet()) {
            WebhookRouteMessage webhookRouteMessage = new WebhookRouteMessage(testCase.getKey(), null);

            Assertions.assertThat(webhookService.hasMalformedRetryEnvelope(webhookRouteMessage)).isEqualTo(testCase.getValue());
        }
    }

    @ParameterizedTest
    @CsvSource({
            "2026-07-10T12:00:30Z, 30",
            "2026-07-10T12:00:00Z, 0",
            "2026-07-10T11:59:59.999999999Z, 0",
            ", 0"
    })
    void calculatesEligibilityWaitBeforeAtAndAfterTheEligibleInstant(Instant nextEligibleAt, long expectedWaitSeconds) {
        Instant now = Instant.parse("2026-07-10T12:00:00Z");
        WebhookService webhookService = webhookService(new ObjectMapper(), WebhookPropertiesFixtures.create(), Mockito.mock(ProducerTemplate.class), Clock.fixed(now, ZoneOffset.UTC));
        WebhookRouteMessage webhookRouteMessage = new WebhookRouteMessage(webhookEvent(0, 0, 0, nextEligibleAt), null);

        Assertions.assertThat(webhookService.eligibilityWait(webhookRouteMessage)).isEqualTo(Duration.ofSeconds(expectedWaitSeconds));
    }

    @Test
    void restoresInterruptionAndThrowsWhenEligibilityWaitingIsInterrupted() {
        Instant now = Instant.parse("2026-07-10T12:00:00Z");
        WebhookService webhookService = webhookService(new ObjectMapper(), WebhookPropertiesFixtures.create(), Mockito.mock(ProducerTemplate.class), Clock.fixed(now, ZoneOffset.UTC));
        WebhookRouteMessage webhookRouteMessage = new WebhookRouteMessage(webhookEvent(0, 0, 0, now.plusSeconds(30)), null);

        Thread.currentThread().interrupt();
        try {
            EligibilityWaitInterruptedException eligibilityWaitInterruptedException = Assertions.catchThrowableOfType(EligibilityWaitInterruptedException.class, () -> webhookService.awaitEligibility(webhookRouteMessage));

            Assertions.assertThat(eligibilityWaitInterruptedException).hasMessage("Eligibility wait was interrupted").hasCauseInstanceOf(InterruptedException.class);
            Assertions.assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void timestampsThePublishedEventUsingTheInjectedClock() {
        ProducerTemplate producerTemplate = Mockito.mock(ProducerTemplate.class);
        ObjectMapper objectMapper = new ObjectMapper();
        Instant createdAt = Instant.parse("2026-07-10T12:00:00Z");
        WebhookService webhookService = webhookService(objectMapper, WebhookPropertiesFixtures.create(), producerTemplate, Clock.fixed(createdAt, ZoneOffset.UTC));

        webhookService.ingestEvent(new EventIngestRequest("order.created", objectMapper.createObjectNode()));

        ArgumentCaptor<WebhookEvent> webhookEventCaptor = ArgumentCaptor.forClass(WebhookEvent.class);
        Mockito.verify(producerTemplate).sendBodyAndHeader(Mockito.anyString(), webhookEventCaptor.capture(), Mockito.anyString(), Mockito.any());
        Assertions.assertThat(webhookEventCaptor.getValue().getCreatedAt()).isEqualTo(createdAt);
    }

    @Test
    void restoresInterruptionAndWrapsInterruptedPublication() {
        ProducerTemplate producerTemplate = Mockito.mock(ProducerTemplate.class);
        ObjectMapper objectMapper = new ObjectMapper();
        WebhookService webhookService = webhookService(objectMapper, WebhookPropertiesFixtures.create(), producerTemplate, Clock.systemUTC());
        EventIngestRequest eventIngestRequest = new EventIngestRequest("order.created", objectMapper.createObjectNode());
        Mockito.doThrow(new RuntimeException(new InterruptedException())).when(producerTemplate).sendBodyAndHeader(Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.any());

        try {
            Assertions.assertThatThrownBy(() -> webhookService.ingestEvent(eventIngestRequest)).isInstanceOf(EventPublicationException.class);
            Assertions.assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void restoresInterruptionAndDropsSensitiveCausesFromDeadLetterPublicationFailure() {
        ProducerTemplate producerTemplate = Mockito.mock(ProducerTemplate.class);
        WebhookService webhookService = webhookService(new ObjectMapper(), WebhookPropertiesFixtures.create(1), producerTemplate, Clock.systemUTC());
        WebhookEvent completedWebhookEvent = event(1);
        WebhookRouteMessage webhookRouteMessage = new WebhookRouteMessage(completedWebhookEvent, retryableResult());
        RuntimeException rawException = new RuntimeException("payload-marker", new InterruptedException("signing-secret"));
        rawException.addSuppressed(new IllegalStateException("suppressed payload-marker"));
        Mockito.doThrow(rawException).when(producerTemplate).sendBodyAndHeader("kafka:webhook-events-dead-letter", completedWebhookEvent, KafkaConstants.KEY, completedWebhookEvent.getEndpointId());

        try {
            Throwable throwable = Assertions.catchThrowable(() -> webhookService.publishDeadLetter(webhookRouteMessage));
            Assertions.assertThat(throwable).isInstanceOf(IllegalStateException.class).hasMessage("Dead-letter publication failed [topic=webhook-events-dead-letter]").hasNoCause();
            Assertions.assertThat(throwable.getSuppressed()).isEmpty();
            Assertions.assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    private static WebhookService service(ObjectMapper objectMapper) {
        return service(objectMapper, WebhookPropertiesFixtures.create());
    }

    private static WebhookService service(ObjectMapper objectMapper, WebhookProperties webhookProperties) {
        return webhookService(objectMapper, webhookProperties, Mockito.mock(ProducerTemplate.class), Clock.systemUTC());
    }

    private static WebhookService webhookService(ObjectMapper objectMapper, WebhookProperties webhookProperties, ProducerTemplate producerTemplate, Clock clock) {
        return new WebhookService(Mockito.mock(RestComponent.class), Mockito.mock(DeliveryAttemptRepository.class), objectMapper, webhookProperties, KAFKA_CONSUMER_PROPERTIES, producerTemplate, clock);
    }

    private static WebhookEvent event() {
        return webhookEvent(0, 0, 0, null);
    }

    private static WebhookEvent event(int deliveryAttemptCount) {
        return webhookEvent(deliveryAttemptCount, 0, 0, null);
    }

    private static WebhookEvent event(int deliveryAttemptCount, int retryCount, int replayCount) {
        return webhookEvent(deliveryAttemptCount, retryCount, replayCount, null);
    }

    private static WebhookEvent webhookEvent(int deliveryAttemptCount, int retryCount, int replayCount, Instant nextEligibleAt) {
        return new WebhookEvent(
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "order.created",
                "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "cccccccc-cccc-cccc-cccc-cccccccccccc",
                Instant.EPOCH,
                ByteBuffer.wrap("{\"secret\":\"payload-marker\",\"key\":\"signing-secret\"}".getBytes(StandardCharsets.UTF_8)),
                retryCount,
                nextEligibleAt,
                deliveryAttemptCount,
                replayCount
        );
    }

    private static byte[] bytes(ByteBuffer sourceByteBuffer) {
        ByteBuffer duplicateByteBuffer = sourceByteBuffer.duplicate();
        byte[] bytes = new byte[duplicateByteBuffer.remaining()];
        duplicateByteBuffer.get(bytes);
        return bytes;
    }
}
