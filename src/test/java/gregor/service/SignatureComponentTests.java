package gregor.service;

import gregor.component.SignatureComponent;
import gregor.config.WebhookPropertiesFixtures;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Base64;

class SignatureComponentTests {

    @Test
    void signsTheExactStandardWebhooksPrefixAndUnchangedBody() throws Exception {
        byte[] secret = "01234567890123456789012345678901".getBytes(StandardCharsets.UTF_8);
        String configuredSecret = "whsec_" + Base64.getEncoder().encodeToString(secret);
        SignatureComponent service = new SignatureComponent(WebhookPropertiesFixtures.create(
                configuredSecret,
                Duration.ofMinutes(5)
        ));
        byte[] body = new byte[]{'{', '"', 'x', '"', ':', (byte) 0xc3, (byte) 0xa9, '}'};

        String signature = service.sign("event-123", 1_700_000_000L, body);

        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secret, "HmacSHA256"));
        mac.update("event-123.1700000000.".getBytes(StandardCharsets.UTF_8));
        byte[] expectedDigest = mac.doFinal(body);
        byte[] actualDigest = Base64.getDecoder().decode(signature.substring("v1,".length()));
        Assertions.assertThat(MessageDigest.isEqual(actualDigest, expectedDigest)).isTrue();
        Assertions.assertThat(signature).startsWith("v1,");
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
