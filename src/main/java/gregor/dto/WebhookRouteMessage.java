package gregor.dto;

import gregor.avro.WebhookEvent;
import jakarta.validation.constraints.NotNull;

public record WebhookRouteMessage(
        @NotNull WebhookEvent webhookEvent,
        DeliveryAttemptResult deliveryAttemptResult
) {}
