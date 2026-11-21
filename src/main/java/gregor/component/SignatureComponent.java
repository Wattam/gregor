package gregor.component;

import gregor.config.properties.WebhookProperties;
import org.apache.commons.lang3.Strings;
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
        byte[] secretBytes = decodeSecret(webhookProperties.signingSecret());
        this.secretKeySpec = new SecretKeySpec(secretBytes, ALGORITHM);
    }

    public String sign(String webhookId, long webhookTimestamp, byte[] bodyBytes) {
        byte[] digestBytes = this.digest(webhookId, webhookTimestamp, bodyBytes);
        return "v1," + Base64.getEncoder().encodeToString(digestBytes);
    }

    private byte[] digest(String webhookId, long webhookTimestamp, byte[] bodyBytes) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(this.secretKeySpec);
            String signedMessagePrefix = webhookId + "." + webhookTimestamp + ".";
            mac.update(signedMessagePrefix.getBytes(StandardCharsets.UTF_8));
            return mac.doFinal(bodyBytes);
        } catch (NoSuchAlgorithmException | InvalidKeyException securityException) {
            throw new IllegalStateException("HMAC-SHA256 is unavailable", securityException);
        }
    }

    private static byte[] decodeSecret(String configuredSecret) {
        if (!Strings.CS.startsWith(configuredSecret, SECRET_PREFIX)) {
            throw new IllegalArgumentException("Webhook signing secret must use the whsec_ prefix");
        }

        byte[] secretBytes;
        try {
            secretBytes = Base64.getDecoder().decode(configuredSecret.substring(SECRET_PREFIX.length()));
        } catch (IllegalArgumentException illegalArgumentException) {
            throw new IllegalArgumentException("Webhook signing secret must contain valid Base64", illegalArgumentException);
        }
        if (secretBytes.length < MINIMUM_SECRET_BYTES || secretBytes.length > MAXIMUM_SECRET_BYTES) {
            throw new IllegalArgumentException("Webhook signing secret must decode to between 24 and 64 bytes");
        }
        return secretBytes;
    }
}
