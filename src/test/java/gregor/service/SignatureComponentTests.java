package gregor.service;

import gregor.component.SignatureComponent;
import gregor.config.WebhookPropertiesFixtures;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

class SignatureComponentTests {

    @Test
    void matchesAStandardWebhooksHmacTestVector() {
        byte[] secret = "01234567890123456789012345678901".getBytes(StandardCharsets.UTF_8);
        String configuredSecret = "whsec_" + Base64.getEncoder().encodeToString(secret);
        SignatureComponent service = new SignatureComponent(WebhookPropertiesFixtures.create(
                configuredSecret,
                Duration.ofMinutes(5)
        ));
        byte[] body = "{\"x\":\"é\"}".getBytes(StandardCharsets.UTF_8);

        String signature = service.sign("event-123", 1_700_000_000L, body);

        Assertions.assertThat(signature).isEqualTo("v1,mhwyQeB5kaTceeNCFewr2qPTce8jPJfx5RT7Hd6SRQQ=");
    }

    @Test
    void rejectsSecretsWithoutThePrefixValidBase64OrRequiredDecodedLength() {
        String tooShort = "whsec_" + Base64.getEncoder().encodeToString(new byte[23]);
        String tooLong = "whsec_" + Base64.getEncoder().encodeToString(new byte[65]);

        Assertions.assertThatThrownBy(() -> new SignatureComponent(
                WebhookPropertiesFixtures.create("secret", Duration.ofMinutes(5))
        )).isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThatThrownBy(() -> new SignatureComponent(
                WebhookPropertiesFixtures.create("whsec_!", Duration.ofMinutes(5))
        )).isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThatThrownBy(() -> new SignatureComponent(
                WebhookPropertiesFixtures.create(tooShort, Duration.ofMinutes(5))
        )).isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThatThrownBy(() -> new SignatureComponent(
                WebhookPropertiesFixtures.create(tooLong, Duration.ofMinutes(5))
        )).isInstanceOf(IllegalArgumentException.class);
    }
}
