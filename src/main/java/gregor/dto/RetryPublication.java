package gregor.dto;

import gregor.avro.WebhookEvent;

public record RetryPublication(String topic, WebhookEvent webhookEvent) {}
