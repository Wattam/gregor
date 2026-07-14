package gregor.config.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.net.URI;
import java.time.Duration;

@ConfigurationProperties("gregor.webhook")
public record WebhookProperties(
        String tenantId,
        String endpointId,
        URI receiverUrl,
        String signingSecret,
        Duration connectionTimeout,
        Duration responseTimeout,
        Duration receiverTimestampTolerance,
        int maximumAttempts
) {}
