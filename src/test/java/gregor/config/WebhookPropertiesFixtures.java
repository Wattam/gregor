package gregor.config;

import gregor.config.properties.WebhookProperties;
import lombok.NoArgsConstructor;

import java.net.URI;
import java.time.Duration;

@NoArgsConstructor
public final class WebhookPropertiesFixtures {

    private static final String DEFAULT_SIGNING_SECRET = "whsec_test-signing-secret";

    public static WebhookProperties create() {
        return create(DEFAULT_SIGNING_SECRET, Duration.ofMinutes(5), 3);
    }

    public static WebhookProperties create(int maximumAttempts) {
        return create(DEFAULT_SIGNING_SECRET, Duration.ofMinutes(5), maximumAttempts);
    }

    public static WebhookProperties create(String signingSecret, Duration receiverTimestampTolerance) {
        return create(signingSecret, receiverTimestampTolerance, 3);
    }

    private static WebhookProperties create(String signingSecret, Duration receiverTimestampTolerance, int maximumAttempts) {
        return new WebhookProperties(
                "00000000-0000-0000-0000-000000000001",
                "00000000-0000-0000-0000-000000000002",
                URI.create("https://receiver.example.test/hooks"),
                signingSecret,
                Duration.ofSeconds(2),
                Duration.ofSeconds(5),
                receiverTimestampTolerance,
                maximumAttempts
        );
    }
}
