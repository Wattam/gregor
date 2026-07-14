package gregor.service;

import gregor.avro.DeliveryAttempt;
import gregor.avro.WebhookEvent;
import gregor.component.RestComponent;
import gregor.config.WebhookPropertiesFixtures;
import gregor.config.properties.WebhookProperties;
import gregor.dto.DeliveryAttemptResult;
import gregor.dto.DeliveryOutcome;
import gregor.dto.EventIngestRequest;
import gregor.dto.WebhookRouteMessage;
import gregor.exception.EventPublicationException;
import gregor.repository.DeliveryAttemptRepository;
import gregor.route.WebhookRoute;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.kafka.KafkaConstants;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;

class WebhookServiceTests {

    @Test
    void deliveryOutcomeDefinesOnlyTheThreePhaseOneResults() {
        Assertions.assertThat(DeliveryOutcome.values()).containsExactly(
                DeliveryOutcome.SUCCEEDED,
                DeliveryOutcome.RETRYABLE_FAILURE,
                DeliveryOutcome.PERMANENT_FAILURE
        );
    }

    @Test
    void createsAnInitialRouteMessage() {
        WebhookService webhookService = service(new ObjectMapper());
        WebhookEvent webhookEvent = event();

        WebhookRouteMessage initialMessage = webhookService.createWebhookRouteMessage(webhookEvent);

        Assertions.assertThat(initialMessage).isEqualTo(new WebhookRouteMessage(webhookEvent, null, 0));
    }

    @Test
    void sendsTheWebhookAndAdvancesTheRouteMessage() {
        RestComponent restComponent = Mockito.mock(RestComponent.class);
        WebhookService webhookService = new WebhookService(
                restComponent,
                Mockito.mock(DeliveryAttemptRepository.class),
                new ObjectMapper(),
                WebhookPropertiesFixtures.create(),
                Mockito.mock(ProducerTemplate.class),
                Clock.systemUTC()
        );
        WebhookEvent webhookEvent = event();
        DeliveryAttemptResult result = new DeliveryAttemptResult(
                DeliveryOutcome.SUCCEEDED,
                204,
                1L,
                Instant.EPOCH,
                Map.of()
        );
        WebhookRouteMessage input = new WebhookRouteMessage(webhookEvent, null, 2);
        Mockito.when(restComponent.sendWebhook(webhookEvent)).thenReturn(result);

        WebhookRouteMessage output = webhookService.sendWebhookEvent(input);

        Assertions.assertThat(output).isEqualTo(new WebhookRouteMessage(webhookEvent, result, 3));
        Mockito.verify(restComponent).sendWebhook(webhookEvent);
    }

    @Test
    void createsACompleteAttemptWithoutSensitiveEventData() {
        ObjectMapper objectMapper = new ObjectMapper();
        WebhookService webhookService = service(objectMapper);
        WebhookEvent webhookEvent = event();
        DeliveryAttemptResult deliveryAttemptResult = new DeliveryAttemptResult(
                DeliveryOutcome.RETRYABLE_FAILURE,
                503,
                37L,
                Instant.parse("2026-07-10T12:00:00Z"),
                Map.of("type", "HTTP_FAILURE", "responseBody", "unavailable")
        );

        WebhookRouteMessage inputMessage = new WebhookRouteMessage(webhookEvent, deliveryAttemptResult, 2);

        DeliveryAttempt deliveryAttempt = webhookService.createAttempt(inputMessage);

        Assertions.assertThatCode(() -> UUID.fromString(deliveryAttempt.getAttemptId())).doesNotThrowAnyException();
        Assertions.assertThat(deliveryAttempt.getEventId()).isEqualTo("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        Assertions.assertThat(deliveryAttempt.getTenantId()).isEqualTo("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
        Assertions.assertThat(deliveryAttempt.getEndpointId()).isEqualTo("cccccccc-cccc-cccc-cccc-cccccccccccc");
        Assertions.assertThat(deliveryAttempt.getEventType()).isEqualTo("order.created");
        Assertions.assertThat(deliveryAttempt.getAttemptNumber()).isEqualTo(2);
        Assertions.assertThat(deliveryAttempt.getOutcome()).isEqualTo(gregor.avro.DeliveryOutcome.RETRYABLE_FAILURE);
        Assertions.assertThat(deliveryAttempt.getHttpStatus()).isEqualTo(503);
        Assertions.assertThat(deliveryAttempt.getLatencyMs()).isEqualTo(37L);
        Assertions.assertThat(deliveryAttempt.getAttemptedAt()).isEqualTo(Instant.parse("2026-07-10T12:00:00Z"));
        byte[] detailsBytes = bytes(deliveryAttempt.getDetails());
        JsonNode details = objectMapper.readTree(detailsBytes);
        Assertions.assertThat(details.get("type").asString()).isEqualTo("HTTP_FAILURE");
        Assertions.assertThat(details.get("responseBody").asString()).isEqualTo("unavailable");
        Assertions.assertThat(new String(detailsBytes, StandardCharsets.UTF_8))
                .doesNotContain("payload-marker", "signing-secret");
        Assertions.assertThat(DeliveryAttempt.getClassSchema().getField("payload")).isNull();
        Assertions.assertThat(DeliveryAttempt.getClassSchema().getField("signingSecret")).isNull();
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

        List<DeliveryAttempt> deliveryAttempts = outcomes.keySet().stream()
                .map(deliveryOutcome -> webhookService.createAttempt(new WebhookRouteMessage(
                        webhookEvent,
                        new DeliveryAttemptResult(deliveryOutcome, null, 1L, Instant.EPOCH, Map.of()),
                        1
                )))
                .toList();

        Assertions.assertThat(deliveryAttempts)
                .extracting(DeliveryAttempt::getOutcome)
                .containsExactlyInAnyOrderElementsOf(outcomes.values());
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
                Map.of()
        );
        IllegalStateException failure = new IllegalStateException("serialization failed");
        Mockito.when(objectMapper.writeValueAsBytes(deliveryAttemptResult.details())).thenThrow(failure);

        WebhookRouteMessage webhookRouteMessage = new WebhookRouteMessage(event(), deliveryAttemptResult, 1);

        Assertions.assertThatThrownBy(() -> webhookService.createAttempt(webhookRouteMessage)).isSameAs(failure);
    }

    @Test
    void retriesOnlyRetryableFailuresBeforeTheConfiguredMaximumAttempt() {
        WebhookProperties webhookProperties = WebhookPropertiesFixtures.create(5);
        WebhookService webhookService = service(new ObjectMapper(), webhookProperties);

        for (int attemptNumber = 1; attemptNumber <= webhookProperties.maximumAttempts(); attemptNumber++) {
            for (DeliveryOutcome deliveryOutcome : DeliveryOutcome.values()) {
                DeliveryAttemptResult deliveryAttemptResult = new DeliveryAttemptResult(
                        deliveryOutcome,
                        null,
                        1L,
                        Instant.EPOCH,
                        Map.of()
                );

                WebhookRouteMessage webhookRouteMessage = new WebhookRouteMessage(
                        event(),
                        deliveryAttemptResult,
                        attemptNumber
                );

                Assertions.assertThat(webhookService.shouldRetry(webhookRouteMessage)).isEqualTo(
                        deliveryOutcome == DeliveryOutcome.RETRYABLE_FAILURE
                                && attemptNumber < webhookProperties.maximumAttempts()
                );
            }
        }
    }

    @Test
    void publishesOneEndpointKeyedEnvelopeAndWaitsForAcknowledgement() {
        ProducerTemplate producerTemplate = Mockito.mock(ProducerTemplate.class);
        ObjectMapper objectMapper = Mockito.spy(new ObjectMapper());
        Clock clock = Clock.fixed(Instant.parse("2026-07-10T12:00:00Z"), ZoneOffset.UTC);
        WebhookService webhookService = service(
                objectMapper,
                WebhookPropertiesFixtures.create(),
                producerTemplate,
                clock
        );
        JsonNode payload = objectMapper.readTree("{\"orderId\":\"123\"}");
        EventIngestRequest eventIngestRequest = new EventIngestRequest("order.created", payload);

        UUID eventId = webhookService.ingestEvent(eventIngestRequest);

        ArgumentCaptor<Object> envelopeCaptor = ArgumentCaptor.forClass(Object.class);
        Mockito.verify(producerTemplate).sendBodyAndHeader(
                Mockito.eq("kafka:" + WebhookRoute.WEBHOOK_EVENT_TOPIC),
                envelopeCaptor.capture(),
                Mockito.eq(KafkaConstants.KEY),
                Mockito.eq("00000000-0000-0000-0000-000000000002")
        );
        WebhookEvent webhookEventEnvelope = (WebhookEvent) envelopeCaptor.getValue();
        Assertions.assertThat(webhookEventEnvelope.getEventId()).isEqualTo(eventId.toString());
        Assertions.assertThat(webhookEventEnvelope.getEventType()).isEqualTo("order.created");
        Assertions.assertThat(webhookEventEnvelope.getTenantId()).isEqualTo("00000000-0000-0000-0000-000000000001");
        Assertions.assertThat(webhookEventEnvelope.getEndpointId()).isEqualTo("00000000-0000-0000-0000-000000000002");
        Assertions.assertThat(webhookEventEnvelope.getCreatedAt()).isEqualTo(Instant.parse("2026-07-10T12:00:00Z"));
        Assertions.assertThat(webhookEventEnvelope.getRetryCount()).isZero();
        ByteBuffer payloadBuffer = webhookEventEnvelope.getPayload().duplicate();
        byte[] publishedPayload = new byte[payloadBuffer.remaining()];
        payloadBuffer.get(publishedPayload);
        Assertions.assertThat(publishedPayload).isEqualTo("{\"orderId\":\"123\"}".getBytes(StandardCharsets.UTF_8));
        Mockito.verify(objectMapper).writeValueAsBytes(payload);
    }

    @Test
    void generatesANewEventIdForEveryAcceptedRequest() {
        ProducerTemplate producerTemplate = Mockito.mock(ProducerTemplate.class);
        ObjectMapper objectMapper = new ObjectMapper();
        WebhookService webhookService = service(
                objectMapper,
                WebhookPropertiesFixtures.create(),
                producerTemplate,
                Clock.systemUTC()
        );
        EventIngestRequest eventIngestRequest =
                new EventIngestRequest("order.created", objectMapper.createObjectNode());

        UUID firstEventId = webhookService.ingestEvent(eventIngestRequest);
        UUID secondEventId = webhookService.ingestEvent(eventIngestRequest);

        Assertions.assertThat(firstEventId).isNotEqualTo(secondEventId);
    }

    @Test
    void wrapsPublicationFailureWithoutReportingAnEventId() {
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
        Mockito.doThrow(new RuntimeException("broker unavailable")).when(producerTemplate).sendBodyAndHeader(
                Mockito.anyString(),
                Mockito.any(),
                Mockito.anyString(),
                Mockito.any()
        );

        Assertions.assertThatThrownBy(() -> webhookService.ingestEvent(eventIngestRequest))
                .isInstanceOf(EventPublicationException.class);
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
                producerTemplate,
                clock
        );
    }

    private static WebhookEvent event() {
        return new WebhookEvent(
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "order.created",
                "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "cccccccc-cccc-cccc-cccc-cccccccccccc",
                Instant.EPOCH,
                ByteBuffer.wrap(
                        "{\"secret\":\"payload-marker\",\"key\":\"signing-secret\"}".getBytes(StandardCharsets.UTF_8)
                ),
                0
        );
    }

    private static byte[] bytes(ByteBuffer source) {
        ByteBuffer copy = source.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }
}
