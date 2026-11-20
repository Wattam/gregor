package gregor.service;

import gregor.avro.DeliveryAttempt;
import gregor.avro.WebhookEvent;
import gregor.component.RestComponent;
import gregor.config.properties.KafkaConsumerProperties;
import gregor.config.properties.WebhookProperties;
import gregor.dto.DeliveryAttemptResult;
import gregor.dto.DeliveryOutcome;
import gregor.dto.EventIngestRequest;
import gregor.dto.RetryPublication;
import gregor.dto.RetryTier;
import gregor.dto.WebhookRouteMessage;
import gregor.exception.EligibilityWaitInterruptedException;
import gregor.exception.EventPublicationException;
import gregor.repository.DeliveryAttemptRepository;
import gregor.route.WebhookRoute;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Body;
import org.apache.camel.Header;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.kafka.KafkaConstants;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@AllArgsConstructor
public class WebhookService {

    private static final List<RetryTier> RETRY_TIERS = List.of(
            new RetryTier(WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC, Duration.ofMinutes(1)),
            new RetryTier(WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC, Duration.ofMinutes(10)),
            new RetryTier(WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC, Duration.ofHours(1))
    );

    private final RestComponent restComponent;
    private final DeliveryAttemptRepository deliveryAttemptRepository;
    private final ObjectMapper objectMapper;
    private final WebhookProperties webhookProperties;
    private final KafkaConsumerProperties kafkaConsumerProperties;
    private final ProducerTemplate producerTemplate;
    private final Clock clock;

    public UUID ingestEvent(EventIngestRequest eventIngestRequest) {
        UUID eventId = UUID.randomUUID();
        try {
            byte[] payloadBytes = this.objectMapper.writeValueAsBytes(eventIngestRequest.payload());
            WebhookEvent webhookEvent = new WebhookEvent(
                    eventId.toString(),
                    eventIngestRequest.eventType(),
                    this.webhookProperties.tenantId(),
                    this.webhookProperties.endpointId(),
                    Instant.now(this.clock),
                    ByteBuffer.wrap(payloadBytes),
                    0,
                    null,
                    0,
                    0
            );
            this.producerTemplate.sendBodyAndHeader("kafka:" + WebhookRoute.WEBHOOK_EVENT_TOPIC, webhookEvent, KafkaConstants.KEY, this.webhookProperties.endpointId());
            return eventId;
        } catch (Exception exception) {
            restoreInterruptIfNeeded(exception);
            throw new EventPublicationException(exception);
        }
    }

    public WebhookRouteMessage createWebhookRouteMessage(@Body WebhookEvent webhookEvent) {
        return new WebhookRouteMessage(webhookEvent, null);
    }

    public boolean hasImpossibleDeliveryState(@Body WebhookRouteMessage webhookRouteMessage) {
        WebhookEvent webhookEvent = webhookRouteMessage.webhookEvent();
        return webhookEvent.getDeliveryAttemptCount() < 0 || webhookEvent.getRetryCount() < 0 || webhookEvent.getReplayCount() < 0 || webhookEvent.getDeliveryAttemptCount() >= this.webhookProperties.maximumAttempts();
    }

    public boolean hasMalformedRetryEnvelope(@Body WebhookRouteMessage webhookRouteMessage) {
        Instant nextEligibleAt = webhookRouteMessage.webhookEvent().getNextEligibleAt();
        if (this.hasImpossibleDeliveryState(webhookRouteMessage) || Objects.isNull(nextEligibleAt)) {
            return true;
        }
        Instant latestPermittedEligibility = Instant.now(this.clock).plus(this.kafkaConsumerProperties.maximumPermittedWait());
        return nextEligibleAt.isAfter(latestPermittedEligibility);
    }

    public void logDiscardedRetryEnvelope(@Body WebhookRouteMessage webhookRouteMessage, @Header(KafkaConstants.TOPIC) String topicName) {
        WebhookEvent webhookEvent = webhookRouteMessage.webhookEvent();
        log.error(
                "Discarding malformed retry envelope [topic={}] [eventId={}] [tenantId={}] [endpointId={}] [deliveryAttemptCount={}] [retryCount={}] [replayCount={}] [nextEligibleAt={}] [maximumAttempts={}] [maximumPermittedWait={}]",
                topicName,
                webhookEvent.getEventId(),
                webhookEvent.getTenantId(),
                webhookEvent.getEndpointId(),
                webhookEvent.getDeliveryAttemptCount(),
                webhookEvent.getRetryCount(),
                webhookEvent.getReplayCount(),
                webhookEvent.getNextEligibleAt(),
                this.webhookProperties.maximumAttempts(),
                this.kafkaConsumerProperties.maximumPermittedWait()
        );
    }

    public Duration eligibilityWait(@Body WebhookRouteMessage webhookRouteMessage) {
        Instant nextEligibleAt = webhookRouteMessage.webhookEvent().getNextEligibleAt();
        if (Objects.isNull(nextEligibleAt)) {
            return Duration.ZERO;
        }
        Duration remainingWait = Duration.between(Instant.now(this.clock), nextEligibleAt);
        return remainingWait.isNegative() ? Duration.ZERO : remainingWait;
    }

    public void awaitEligibility(@Body WebhookRouteMessage webhookRouteMessage) {
        Duration remainingWait = this.eligibilityWait(webhookRouteMessage);
        if (remainingWait.isZero()) {
            return;
        }
        try {
            Thread.sleep(remainingWait);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new EligibilityWaitInterruptedException(interruptedException);
        }
    }

    public void logDiscardedEnvelope(@Body WebhookRouteMessage webhookRouteMessage) {
        WebhookEvent webhookEvent = webhookRouteMessage.webhookEvent();
        log.error(
                "Discarding webhook event with impossible delivery state [eventId={}] [tenantId={}] [endpointId={}] [deliveryAttemptCount={}] [retryCount={}] [replayCount={}] [maximumAttempts={}]",
                webhookEvent.getEventId(),
                webhookEvent.getTenantId(),
                webhookEvent.getEndpointId(),
                webhookEvent.getDeliveryAttemptCount(),
                webhookEvent.getRetryCount(),
                webhookEvent.getReplayCount(),
                this.webhookProperties.maximumAttempts()
        );
    }

    public WebhookRouteMessage sendWebhookEvent(@Body WebhookRouteMessage webhookRouteMessage) {
        WebhookEvent receivedWebhookEvent = webhookRouteMessage.webhookEvent();
        DeliveryAttemptResult deliveryAttemptResult = this.restComponent.sendWebhook(receivedWebhookEvent);
        WebhookEvent advancedWebhookEvent = WebhookEvent.newBuilder(receivedWebhookEvent).setDeliveryAttemptCount(receivedWebhookEvent.getDeliveryAttemptCount() + 1).build();
        return new WebhookRouteMessage(advancedWebhookEvent, deliveryAttemptResult);
    }

    public DeliveryAttempt createAttempt(@Body WebhookRouteMessage webhookRouteMessage) {
        WebhookEvent webhookEvent = webhookRouteMessage.webhookEvent();
        DeliveryAttemptResult deliveryAttemptResult = webhookRouteMessage.deliveryAttemptResult();
        byte[] detailsBytes = this.objectMapper.writeValueAsBytes(deliveryAttemptResult.details());
        return new DeliveryAttempt(
                UUID.randomUUID().toString(),
                webhookEvent.getEventId(),
                webhookEvent.getTenantId(),
                webhookEvent.getEndpointId(),
                webhookEvent.getEventType(),
                webhookEvent.getDeliveryAttemptCount(),
                mapOutcome(deliveryAttemptResult.outcome()),
                deliveryAttemptResult.httpStatus(),
                deliveryAttemptResult.latencyMs(),
                deliveryAttemptResult.attemptedAt(),
                ByteBuffer.wrap(detailsBytes),
                webhookEvent.getRetryCount(),
                webhookEvent.getReplayCount(),
                this.requiresDeadLetter(webhookRouteMessage)
        );
    }

    public boolean requiresDeadLetter(@Body WebhookRouteMessage webhookRouteMessage) {
        return switch (webhookRouteMessage.deliveryAttemptResult().outcome()) {
            case SUCCEEDED -> false;
            case PERMANENT_FAILURE -> true;
            case RETRYABLE_FAILURE -> this.findNextRetryTier(webhookRouteMessage.webhookEvent()).isEmpty();
        };
    }

    public Optional<RetryPublication> planRetry(@Body WebhookRouteMessage webhookRouteMessage) {
        WebhookEvent attemptedWebhookEvent = webhookRouteMessage.webhookEvent();
        DeliveryAttemptResult deliveryAttemptResult = webhookRouteMessage.deliveryAttemptResult();
        if (deliveryAttemptResult.outcome() != DeliveryOutcome.RETRYABLE_FAILURE) {
            return Optional.empty();
        }

        return this.findNextRetryTier(attemptedWebhookEvent).map(retryTier -> {
            Instant nextEligibleAt = deliveryAttemptResult.completedAt().plus(retryTier.retryDelay()).truncatedTo(ChronoUnit.MILLIS);
            WebhookEvent successorWebhookEvent = WebhookEvent.newBuilder(attemptedWebhookEvent).setRetryCount(attemptedWebhookEvent.getRetryCount() + 1).setNextEligibleAt(nextEligibleAt).build();
            return new RetryPublication(retryTier.topic(), successorWebhookEvent);
        });
    }

    public void publishDeadLetter(@Body WebhookRouteMessage webhookRouteMessage) {
        WebhookEvent webhookEvent = webhookRouteMessage.webhookEvent();
        try {
            this.producerTemplate.sendBodyAndHeader("kafka:" + WebhookRoute.WEBHOOK_EVENT_DEAD_LETTER_TOPIC, webhookEvent, KafkaConstants.KEY, webhookEvent.getEndpointId());
        } catch (Exception exception) {
            restoreInterruptIfNeeded(exception);
            throw new IllegalStateException("Dead-letter publication failed [topic=webhook-events-dead-letter]");
        }
    }

    private Optional<RetryTier> findNextRetryTier(WebhookEvent attemptedWebhookEvent) {
        int retryCount = attemptedWebhookEvent.getRetryCount();
        if (attemptedWebhookEvent.getDeliveryAttemptCount() >= this.webhookProperties.maximumAttempts() || retryCount < 0 || retryCount >= RETRY_TIERS.size()) {
            return Optional.empty();
        }
        return Optional.of(RETRY_TIERS.get(retryCount));
    }

    @Transactional
    public void persistDeliveryAttempt(@Body DeliveryAttempt deliveryAttempt) {
        byte[] detailsBytes = copy(deliveryAttempt.getDetails());
        try {
            this.objectMapper.readTree(detailsBytes);
        } catch (Exception exception) {
            throw new IllegalArgumentException("Delivery attempt details must contain valid JSON", exception);
        }
        String detailsJson = new String(detailsBytes, StandardCharsets.UTF_8);
        int insertedRowCount = this.deliveryAttemptRepository.insertIfAbsent(
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
                detailsJson,
                deliveryAttempt.getRetryCount(),
                deliveryAttempt.getReplayCount(),
                deliveryAttempt.getDeadLettered()
        );
        if (insertedRowCount != 0 && insertedRowCount != 1) {
            throw new IllegalStateException("Unexpected delivery attempt insert count: " + insertedRowCount);
        }
    }

    private static void restoreInterruptIfNeeded(Throwable throwable) {
        for (Throwable currentThrowable = throwable; currentThrowable != null; currentThrowable = currentThrowable.getCause()) {
            if (currentThrowable instanceof InterruptedException) {
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

    private static byte[] copy(ByteBuffer sourceByteBuffer) {
        ByteBuffer duplicateByteBuffer = sourceByteBuffer.duplicate();
        byte[] bytes = new byte[duplicateByteBuffer.remaining()];
        duplicateByteBuffer.get(bytes);
        return bytes;
    }
}
