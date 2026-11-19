package gregor.integration;

import lombok.NoArgsConstructor;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;

@NoArgsConstructor
final class WebhookSignatureFixtures {

    static String expectedSignature(String signingSecret, String eventId, String timestamp, byte[] payloadBytes) {
        byte[] keyBytes = Base64.getDecoder().decode(signingSecret.substring("whsec_".length()));
        byte[] prefixBytes = (eventId + "." + timestamp + ".").getBytes(StandardCharsets.UTF_8);
        byte[] signedBytes = ByteBuffer.allocate(prefixBytes.length + payloadBytes.length).put(prefixBytes).put(payloadBytes).array();
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(keyBytes, "HmacSHA256"));
            return "v1," + Base64.getEncoder().encodeToString(mac.doFinal(signedBytes));
        } catch (GeneralSecurityException generalSecurityException) {
            throw new AssertionError("Could not sign the test payload", generalSecurityException);
        }
    }
}
