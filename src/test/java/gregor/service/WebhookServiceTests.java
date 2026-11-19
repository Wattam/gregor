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
        DeliveryAttemptResult result = new DeliveryAttemptResult(
                DeliveryOutcome.SUCCEEDED,
                204,
                1L,
                Instant.EPOCH,
                Instant.EPOCH.plusSeconds(2),
                Map.of()
        );
        WebhookRouteMessage input = new WebhookRouteMessage(webhookEvent, null);
        Mockito.when(restComponent.sendWebhook(webhookEvent)).thenReturn(result);

        WebhookRouteMessage output = webhookService.sendWebhookEvent(input);

        Assertions.assertThat(output).isEqualTo(new WebhookRouteMessage(event(3, 1, 3), result));
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
                Map.of("type", "HTTP_FAILURE", "responseBody", "unavailable")
        );

        WebhookRouteMessage inputMessage = new WebhookRouteMessage(webhookEvent, deliveryAttemptResult);

        DeliveryAttempt deliveryAttempt = webhookService.createAttempt(inputMessage);

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
        Assertions.assertThat(deliveryAttempt.getDeadLettered()).isFalse();
        byte[] detailsBytes = bytes(deliveryAttempt.getDetails());
        JsonNode details = objectMapper.readTree(detailsBytes);
        Assertions.assertThat(details.get("type").asString()).isEqualTo("HTTP_FAILURE");
        Assertions.assertThat(details.get("responseBody").asString()).isEqualTo("unavailable");
        Assertions.assertThat(new String(detailsBytes, StandardCharsets.UTF_8))
                .doesNotContain("payload-marker", "signing-secret");
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
        outcomes.forEach((input, expected) -> {
            DeliveryAttempt attempt = webhookService.createAttempt(new WebhookRouteMessage(
                    webhookEvent,
                    new DeliveryAttemptResult(input, null, 1L, Instant.EPOCH, Instant.EPOCH, Map.of())
            ));
            Assertions.assertThat(attempt.getOutcome()).as("mapping for %s", input).isEqualTo(expected);
            deliveryAttempts.add(attempt);
        });

        Assertions.assertThat(deliveryAttempts).extracting(DeliveryAttempt::getAttemptId).doesNotHaveDuplicates();
        Assertions.assertThat(deliveryAttempts).extracting(DeliveryAttempt::getHttpStatus).containsOnlyNulls();
    }

    @Test
    void propagatesAttemptDetailsSerializationFailure() {
        ObjectMapper objectMapper = Mockito.mock(ObjectMapper.class);
        WebhookService webhookService = service(objectMapper);
        DeliveryAttemptResult deliveryAttemptResult = new DeliveryAttemptResult(
                DeliveryOutcome.SUCCEEDED,
                204,
                1L,
                Instant.EPOCH,
                Instant.EPOCH.plusSeconds(2),
                Map.of()
        );
        IllegalStateException failure = new IllegalStateException("serialization failed");
        Mockito.when(objectMapper.writeValueAsBytes(deliveryAttemptResult.details())).thenThrow(failure);

        WebhookRouteMessage webhookRouteMessage = new WebhookRouteMessage(event(1), deliveryAttemptResult);

        Assertions.assertThatThrownBy(() -> webhookService.createAttempt(webhookRouteMessage)).isSameAs(failure);
    }

    @ParameterizedTest
    @CsvSource({
            "0, 1, webhook-events-retry-1m, 2026-07-10T12:01:02.123Z",
            "1, 2, webhook-events-retry-10m, 2026-07-10T12:10:02.123Z",
            "2, 3, webhook-events-retry-1h, 2026-07-10T13:00:02.123Z"
    })
    void plansEachTierFromHttpCompletionPreservingTheEnvelope(
            int retryCount, int successorCount, String topic, Instant eligibility
    ) {
        RestComponent restComponent = Mockito.mock(RestComponent.class);
        DeliveryAttemptRepository deliveryAttemptRepository = Mockito.mock(DeliveryAttemptRepository.class);
        ProducerTemplate producerTemplate = Mockito.mock(ProducerTemplate.class);
        Clock clock = Mockito.mock(Clock.class);
        Mockito.when(clock.instant()).thenReturn(Instant.parse("2026-07-10T12:00:10Z"));
        WebhookService webhookService = new WebhookService(
                restComponent,
                deliveryAttemptRepository,
                new ObjectMapper(),
                WebhookPropertiesFixtures.create(4),
                KAFKA_CONSUMER_PROPERTIES,
                producerTemplate,
                clock
        );
        Instant previousEligibility = Instant.parse("2026-07-10T11:55:00Z");
        byte[] payload = {0, -1, 10, 32};
        WebhookEvent input = WebhookEvent
                .newBuilder(event(2, retryCount, 5, previousEligibility))
                .setPayload(ByteBuffer.wrap(payload))
                .build();
        WebhookEvent expected = WebhookEvent
                .newBuilder(input)
                .setRetryCount(successorCount)
                .setNextEligibleAt(eligibility)
                .build();

        Assertions.assertThat(webhookService.planRetry(new WebhookRouteMessage(input, retryableResult())))
                .contains(new RetryPublication(topic, expected));
        Assertions.assertThat(input.getRetryCount()).isEqualTo(retryCount);
        Assertions.assertThat(input.getNextEligibleAt()).isEqualTo(previousEligibility);
        Assertions.assertThat(bytes(input.getPayload())).isEqualTo(payload);
        Mockito.verifyNoInteractions(restComponent, deliveryAttemptRepository, producerTemplate, clock);
    }

    @ParameterizedTest
    @CsvSource({"TIMEOUT", "CONNECTION_FAILURE"})
    void plansARetryForTransportFailuresWithoutAnHttpStatus(String failureType) {
        WebhookService webhookService = service(new ObjectMapper(), WebhookPropertiesFixtures.create(2));
        WebhookEvent input = event(1);
        DeliveryAttemptResult result = new DeliveryAttemptResult(
                DeliveryOutcome.RETRYABLE_FAILURE, null, 2_123L,
                Instant.parse("2026-07-10T12:00:00Z"), Instant.parse("2026-07-10T12:00:02.123456789Z"),
                Map.of("type", failureType)
        );
        WebhookEvent expected = event(1, 1, 0, Instant.parse("2026-07-10T12:01:02.123Z"));

        Assertions.assertThat(webhookService.planRetry(new WebhookRouteMessage(input, result)))
                .contains(new RetryPublication("webhook-events-retry-1m", expected));
    }

    @ParameterizedTest
    @CsvSource({
            "RETRYABLE_FAILURE, 3, 0",
            "RETRYABLE_FAILURE, 4, 0",
            "SUCCEEDED, 2, 0",
            "PERMANENT_FAILURE, 2, 0",
            "RETRYABLE_FAILURE, 2, -1",
            "RETRYABLE_FAILURE, 2, 3",
            "RETRYABLE_FAILURE, 2, 7",
            "RETRYABLE_FAILURE, 2, 2147483647"
    })
    void doesNotPlanASuccessorForTerminalOrUnsupportedState(DeliveryOutcome outcome, int attempt, int retries) {
        WebhookService webhookService = service(new ObjectMapper(), WebhookPropertiesFixtures.create(3));
        WebhookRouteMessage webhookRouteMessage = new WebhookRouteMessage(
                event(attempt, retries, 0),
                new DeliveryAttemptResult(outcome, null, 1L, Instant.EPOCH, Instant.EPOCH, Map.of())
        );

        Assertions.assertThat(webhookService.planRetry(webhookRouteMessage)).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({
            "1, 1, 0, ''",
            "2, 1/2, 0/1, webhook-events-retry-1m",
            "3, 1/2/3, 0/1/2, webhook-events-retry-1m/webhook-events-retry-10m",
            "4, 1/2/3/4, 0/1/2/3, webhook-events-retry-1m/webhook-events-retry-10m/webhook-events-retry-1h"
    })
    void advancesOneCumulativeAttemptAndAuditCountPerEnvelope(
            int maximum, String attemptNumbers, String retryCounts, String tiers
    ) {
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
        ArgumentCaptor<WebhookEvent> ingested = ArgumentCaptor.forClass(WebhookEvent.class);
        Mockito.verify(producerTemplate).sendBodyAndHeader(
                Mockito.anyString(), ingested.capture(), Mockito.anyString(), Mockito.any()
        );
        WebhookEvent next = ingested.getValue();
        List<Integer> attempts = new ArrayList<>();
        List<Integer> retries = new ArrayList<>();
        List<String> topics = new ArrayList<>();
        for (int delivery = 0; delivery < 4 && next != null; delivery++) {
            WebhookRouteMessage result =
                    webhookService.sendWebhookEvent(webhookService.createWebhookRouteMessage(next));
            DeliveryAttempt attempt = webhookService.createAttempt(result);
            attempts.add(attempt.getAttemptNumber());
            retries.add(attempt.getRetryCount());
            RetryPublication successor = webhookService.planRetry(result).orElse(null);
            next = successor == null ? null : successor.webhookEvent();
            if (successor != null) {
                topics.add(successor.topic());
            }
        }

        Assertions.assertThat(attempts).containsExactlyElementsOf(numbers(attemptNumbers));
        Assertions.assertThat(retries).containsExactlyElementsOf(numbers(retryCounts));
        Assertions.assertThat(String.join("/", topics)).isEqualTo(tiers);
        Assertions.assertThat(next).isNull();
        Mockito.verify(restComponent, Mockito.times(maximum)).sendWebhook(Mockito.any());
    }

    private static List<Integer> numbers(String values) {
        return Arrays.stream(values.split("/")).map(Integer::valueOf).toList();
    }

    private static DeliveryAttemptResult retryableResult() {
        return new DeliveryAttemptResult(
                DeliveryOutcome.RETRYABLE_FAILURE,
                503,
                2_123L,
                Instant.parse("2026-07-10T12:00:00Z"),
                Instant.parse("2026-07-10T12:00:02.123456789Z"),
                Map.of()
        );
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
        cases.put(event(0, 0, 0, Instant.parse("2026-07-10T12:00:00Z")), false);

        for (Map.Entry<WebhookEvent, Boolean> testCase : cases.entrySet()) {
            WebhookRouteMessage webhookRouteMessage = new WebhookRouteMessage(testCase.getKey(), null);

            Assertions.assertThat(webhookService.hasImpossibleDeliveryState(webhookRouteMessage))
                    .isEqualTo(testCase.getValue());
        }
    }

    @Test
    void detectsMalformedRetryEnvelopesAtTheMaximumPermittedWaitBoundary() {
        Instant now = Instant.parse("2026-07-10T12:00:00Z");
        WebhookService webhookService = service(
                new ObjectMapper(),
                WebhookPropertiesFixtures.create(4),
                Mockito.mock(ProducerTemplate.class),
                Clock.fixed(now, ZoneOffset.UTC)
        );
        Duration maximumPermittedWait = Duration.ofMinutes(70);
        Map<WebhookEvent, Boolean> cases = new LinkedHashMap<>();
        cases.put(event(0, 0, 0, null), true);
        cases.put(event(4, 0, 0, now), true);
        cases.put(event(-1, 0, 0, now), true);
        cases.put(event(0, -1, 0, now), true);
        cases.put(event(0, 0, -1, now), true);
        cases.put(event(0, 0, 0, now.plus(maximumPermittedWait).plusNanos(1)), true);
        cases.put(event(0, 0, 0, now.plus(maximumPermittedWait)), false);
        cases.put(event(0, 3, 0, now), false);

        for (Map.Entry<WebhookEvent, Boolean> testCase : cases.entrySet()) {
            WebhookRouteMessage webhookRouteMessage = new WebhookRouteMessage(testCase.getKey(), null);

            Assertions.assertThat(webhookService.hasMalformedRetryEnvelope(webhookRouteMessage))
                    .isEqualTo(testCase.getValue());
        }
    }

    @Test
    void calculatesEligibilityWaitBeforeAtAndAfterTheEligibleInstant() {
        Instant now = Instant.parse("2026-07-10T12:00:00Z");
        WebhookService webhookService = service(
                new ObjectMapper(),
                WebhookPropertiesFixtures.create(),
                Mockito.mock(ProducerTemplate.class),
                Clock.fixed(now, ZoneOffset.UTC)
        );

        Assertions.assertThat(webhookService.eligibilityWait(new WebhookRouteMessage(
                event(0, 0, 0, now.plusSeconds(30)),
                null
        ))).isEqualTo(Duration.ofSeconds(30));
        Assertions.assertThat(webhookService.eligibilityWait(new WebhookRouteMessage(
                event(0, 0, 0, now),
                null
        ))).isEqualTo(Duration.ZERO);
        Assertions.assertThat(webhookService.eligibilityWait(new WebhookRouteMessage(
                event(0, 0, 0, now.minusNanos(1)),
                null
        ))).isEqualTo(Duration.ZERO);
        Assertions.assertThat(webhookService.eligibilityWait(new WebhookRouteMessage(
                event(0, 0, 0, null),
                null
        ))).isEqualTo(Duration.ZERO);
    }

    @Test
    void restoresInterruptionAndThrowsWhenEligibilityWaitingIsInterrupted() {
        Instant now = Instant.parse("2026-07-10T12:00:00Z");
        WebhookService webhookService = service(
                new ObjectMapper(),
                WebhookPropertiesFixtures.create(),
                Mockito.mock(ProducerTemplate.class),
                Clock.fixed(now, ZoneOffset.UTC)
        );
        WebhookRouteMessage webhookRouteMessage = new WebhookRouteMessage(
                event(0, 0, 0, now.plusSeconds(30)),
                null
        );

        Thread.currentThread().interrupt();
        try {
            EligibilityWaitInterruptedException eligibilityWaitInterruptedException = Assertions.catchThrowableOfType(
                    EligibilityWaitInterruptedException.class,
                    () -> webhookService.awaitEligibility(webhookRouteMessage)
            );

            Assertions.assertThat(eligibilityWaitInterruptedException)
                    .hasMessage("Eligibility wait was interrupted")
                    .hasCauseInstanceOf(InterruptedException.class);
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
        WebhookService webhookService = service(
                objectMapper,
                WebhookPropertiesFixtures.create(),
                producerTemplate,
                Clock.fixed(createdAt, ZoneOffset.UTC)
        );

        webhookService.ingestEvent(new EventIngestRequest("order.created", objectMapper.createObjectNode()));

        ArgumentCaptor<WebhookEvent> event = ArgumentCaptor.forClass(WebhookEvent.class);
        Mockito.verify(producerTemplate).sendBodyAndHeader(
                Mockito.anyString(), event.capture(), Mockito.anyString(), Mockito.any()
        );
        Assertions.assertThat(event.getValue().getCreatedAt()).isEqualTo(createdAt);
    }

    @Test
    void restoresInterruptionAndWrapsInterruptedPublication() {
        ProducerTemplate producerTemplate = Mockito.mock(ProducerTemplate.class);
        ObjectMapper objectMapper = new ObjectMapper();
        WebhookService webhookService = service(
                objectMapper,
                WebhookPropertiesFixtures.create(),
                producerTemplate,
                Clock.systemUTC()
        );
        EventIngestRequest eventIngestRequest = new EventIngestRequest(
                "order.created",
                objectMapper.createObjectNode()
        );
        Mockito.doThrow(new RuntimeException(new InterruptedException()))
                .when(producerTemplate)
                .sendBodyAndHeader(Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.any());

        try {
            Assertions.assertThatThrownBy(() -> webhookService.ingestEvent(eventIngestRequest))
                    .isInstanceOf(EventPublicationException.class);
            Assertions.assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    private static WebhookService service(ObjectMapper objectMapper) {
        return service(objectMapper, WebhookPropertiesFixtures.create());
    }

    private static WebhookService service(ObjectMapper objectMapper, WebhookProperties webhookProperties) {
        return service(objectMapper, webhookProperties, Mockito.mock(ProducerTemplate.class), Clock.systemUTC());
    }

    private static WebhookService service(
            ObjectMapper objectMapper,
            WebhookProperties webhookProperties,
            ProducerTemplate producerTemplate,
            Clock clock
    ) {
        return new WebhookService(
                Mockito.mock(RestComponent.class),
                Mockito.mock(DeliveryAttemptRepository.class),
                objectMapper,
                webhookProperties,
                KAFKA_CONSUMER_PROPERTIES,
                producerTemplate,
                clock
        );
    }

    private static WebhookEvent event() {
        return event(0, 0, 0, null);
    }

    private static WebhookEvent event(int deliveryAttemptCount) {
        return event(deliveryAttemptCount, 0, 0, null);
    }

    private static WebhookEvent event(int deliveryAttemptCount, int retryCount, int replayCount) {
        return event(deliveryAttemptCount, retryCount, replayCount, null);
    }

    private static WebhookEvent event(
            int deliveryAttemptCount,
            int retryCount,
            int replayCount,
            Instant nextEligibleAt
    ) {
        return new WebhookEvent(
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "order.created",
                "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "cccccccc-cccc-cccc-cccc-cccccccccccc",
                Instant.EPOCH,
                ByteBuffer.wrap(
                        "{\"secret\":\"payload-marker\",\"key\":\"signing-secret\"}".getBytes(StandardCharsets.UTF_8)
                ),
                retryCount,
                nextEligibleAt,
                deliveryAttemptCount,
                replayCount
        );
    }

    private static byte[] bytes(ByteBuffer source) {
        ByteBuffer copy = source.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }
}
