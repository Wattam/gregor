package gregor.config.properties;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.net.URI;
import java.time.Duration;

@Validated
@ConfigurationProperties("gregor.webhook")
public record WebhookProperties(String tenantId, String endpointId, URI receiverUrl, String signingSecret, Duration connectionTimeout, Duration responseTimeout, Duration receiverTimestampTolerance, @Min(1) @Max(4) int maximumAttempts) {}
