package gregor.service;

import gregor.avro.DeliveryAttempt;
import gregor.avro.WebhookEvent;
import gregor.component.RestComponent;
import gregor.config.properties.WebhookProperties;
import gregor.dto.DeliveryAttemptResult;
import gregor.dto.DeliveryOutcome;
import gregor.dto.EventIngestRequest;
import gregor.dto.WebhookRouteMessage;
import gregor.exception.EventPublicationException;
import gregor.repository.DeliveryAttemptRepository;
import gregor.route.WebhookRoute;
import lombok.AllArgsConstructor;
import org.apache.camel.Body;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.kafka.KafkaConstants;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

@Service
@AllArgsConstructor
public class WebhookService {

    private final RestComponent restComponent;
    private final DeliveryAttemptRepository deliveryAttemptRepository;
    private final ObjectMapper objectMapper;
    private final WebhookProperties webhookProperties;
    private final ProducerTemplate producerTemplate;
    private final Clock clock;

    public UUID ingestEvent(EventIngestRequest eventIngestRequest) {
        UUID eventId = UUID.randomUUID();
        try {
            byte[] payload = this.objectMapper.writeValueAsBytes(eventIngestRequest.payload());
            WebhookEvent webhookEvent = new WebhookEvent(
                    eventId.toString(),
                    eventIngestRequest.eventType(),
                    this.webhookProperties.tenantId(),
                    this.webhookProperties.endpointId(),
                    Instant.now(this.clock),
                    ByteBuffer.wrap(payload),
                    0
            );
            this.producerTemplate.sendBodyAndHeader(
                    "kafka:" + WebhookRoute.WEBHOOK_EVENT_TOPIC,
                    webhookEvent,
                    KafkaConstants.KEY,
                    this.webhookProperties.endpointId()
            );
            return eventId;
        } catch (Exception exception) {
            restoreInterruptIfNeeded(exception);
            throw new EventPublicationException(exception);
        }
    }

    public WebhookRouteMessage createWebhookRouteMessage(@Body WebhookEvent webhookEvent) {
        return new WebhookRouteMessage(webhookEvent, null, 0);
    }

    public WebhookRouteMessage sendWebhookEvent(@Body WebhookRouteMessage webhookRouteMessage) {
        WebhookEvent webhookEvent = webhookRouteMessage.webhookEvent();
        DeliveryAttemptResult deliveryAttemptResult = this.restComponent.sendWebhook(webhookEvent);
        return new WebhookRouteMessage(webhookEvent, deliveryAttemptResult, webhookRouteMessage.attemptNumber() + 1);
    }

    public DeliveryAttempt createAttempt(@Body WebhookRouteMessage webhookRouteMessage) {
        WebhookEvent event = webhookRouteMessage.webhookEvent();
        DeliveryAttemptResult result = webhookRouteMessage.deliveryAttemptResult();
        byte[] details = this.objectMapper.writeValueAsBytes(result.details());
        return new DeliveryAttempt(
                UUID.randomUUID().toString(),
                event.getEventId(),
                event.getTenantId(),
                event.getEndpointId(),
                event.getEventType(),
                webhookRouteMessage.attemptNumber(),
                mapOutcome(result.outcome()),
                result.httpStatus(),
                result.latencyMs(),
                result.attemptedAt(),
                ByteBuffer.wrap(details)
        );
    }

    public boolean shouldRetry(@Body WebhookRouteMessage webhookRouteMessage) {
        return webhookRouteMessage.deliveryAttemptResult().outcome() == DeliveryOutcome.RETRYABLE_FAILURE
                && webhookRouteMessage.attemptNumber() < this.webhookProperties.maximumAttempts();
    }

    @Transactional
    public void persistDeliveryAttempt(@Body DeliveryAttempt deliveryAttempt) {
        byte[] detailBytes = copy(deliveryAttempt.getDetails());
        try {
            this.objectMapper.readTree(detailBytes);
        } catch (Exception exception) {
            throw new IllegalArgumentException("Delivery attempt details must contain valid JSON", exception);
        }
        String details = new String(detailBytes, StandardCharsets.UTF_8);
        int inserted = this.deliveryAttemptRepository.insertIfAbsent(
                UUID.fromString(deliveryAttempt.getAttemptId()),
                UUID.fromString(deliveryAttempt.getEventId()),
                UUID.fromString(deliveryAttempt.getTenantId()),
                UUID.fromString(deliveryAttempt.getEndpointId()),
                deliveryAttempt.getEventType(),
                deliveryAttempt.getAttemptNumber(),
                deliveryAttempt.getOutcome().name(),
                deliveryAttempt.getHttpStatus(),
                deliveryAttempt.getLatencyMs(),
                deliveryAttempt.getAttemptedAt(),
                details
        );
        if (inserted != 0 && inserted != 1) {
            throw new IllegalStateException("Unexpected delivery attempt insert count: " + inserted);
        }
    }

    private static void restoreInterruptIfNeeded(Throwable throwable) {
        for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
            if (cause instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static gregor.avro.DeliveryOutcome mapOutcome(DeliveryOutcome deliveryOutcome) {
        return switch (deliveryOutcome) {
            case SUCCEEDED -> gregor.avro.DeliveryOutcome.SUCCEEDED;
            case RETRYABLE_FAILURE -> gregor.avro.DeliveryOutcome.RETRYABLE_FAILURE;
            case PERMANENT_FAILURE -> gregor.avro.DeliveryOutcome.PERMANENT_FAILURE;
        };
    }

    private static byte[] copy(ByteBuffer source) {
        ByteBuffer copy = source.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }
}
