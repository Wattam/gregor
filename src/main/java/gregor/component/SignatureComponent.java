package gregor.component;

import gregor.config.properties.WebhookProperties;
import org.springframework.stereotype.Component;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

@Component
public class SignatureComponent {

    private static final String SECRET_PREFIX = "whsec_";
    private static final String ALGORITHM = "HmacSHA256";
    private static final int MINIMUM_SECRET_BYTES = 24;
    private static final int MAXIMUM_SECRET_BYTES = 64;

    private final SecretKeySpec secretKeySpec;

    public SignatureComponent(WebhookProperties webhookProperties) {
        byte[] secret = decodeSecret(webhookProperties.signingSecret());
        this.secretKeySpec = new SecretKeySpec(secret, ALGORITHM);
    }

    public String sign(String webhookId, long timestamp, byte[] body) {
        byte[] digest = this.digest(webhookId, timestamp, body);
        return "v1," + Base64.getEncoder().encodeToString(digest);
    }

    private byte[] digest(String webhookId, long timestamp, byte[] body) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(this.secretKeySpec);
            String prefix = webhookId + "." + timestamp + ".";
            mac.update(prefix.getBytes(StandardCharsets.UTF_8));
            return mac.doFinal(body);
        } catch (NoSuchAlgorithmException | InvalidKeyException exception) {
            throw new IllegalStateException("HMAC-SHA256 is unavailable", exception);
        }
    }

    private static byte[] decodeSecret(String configuredSecret) {
        if (configuredSecret == null || !configuredSecret.startsWith(SECRET_PREFIX)) {
            throw new IllegalArgumentException("Webhook signing secret must use the whsec_ prefix");
        }

        byte[] secret;
        try {
            secret = Base64.getDecoder().decode(configuredSecret.substring(SECRET_PREFIX.length()));
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("Webhook signing secret must contain valid Base64", exception);
        }
        if (secret.length < MINIMUM_SECRET_BYTES || secret.length > MAXIMUM_SECRET_BYTES) {
            throw new IllegalArgumentException("Webhook signing secret must decode to between 24 and 64 bytes");
        }
        return secret;
    }
}
