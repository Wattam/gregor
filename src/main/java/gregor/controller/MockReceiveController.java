package gregor.controller;

import gregor.component.SignatureComponent;
import gregor.config.properties.WebhookProperties;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.security.MessageDigest;
import java.time.Clock;
import java.util.Base64;

@RestController
@RequestMapping("/api/mocked-webhook-receiver")
public class MockReceiveController {

    private static final String SIGNATURE_PREFIX = "v1,";

    private final SignatureComponent signatureComponent;
    private final long timestampToleranceSeconds;
    private final Clock clock;

    public MockReceiveController(SignatureComponent signatureComponent, WebhookProperties webhookProperties, Clock clock) {
        if (webhookProperties.receiverTimestampTolerance().isNegative()) {
            throw new IllegalArgumentException("Webhook timestamp tolerance must not be negative");
        }
        this.signatureComponent = signatureComponent;
        this.timestampToleranceSeconds = webhookProperties.receiverTimestampTolerance().toSeconds();
        this.clock = clock;
    }

    @PostMapping
    public ResponseEntity<Void> receive(
            @RequestBody byte[] bodyBytes,
            @RequestHeader(name = "webhook-id", required = false) String webhookId,
            @RequestHeader(name = "webhook-timestamp", required = false) String timestampHeader,
            @RequestHeader(name = "webhook-signature", required = false) String signatureHeader
    ) {
        if (this.verify(webhookId, timestampHeader, signatureHeader, bodyBytes)) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.status(401).build();
    }

    private boolean verify(String webhookId, String timestampHeader, String signatureHeader, byte[] bodyBytes) {
        if (webhookId == null || timestampHeader == null || signatureHeader == null || bodyBytes == null) {
            return false;
        }

        long webhookTimestamp;
        try {
            webhookTimestamp = Long.parseLong(timestampHeader);
        } catch (NumberFormatException _) {
            return false;
        }
        long currentTimestamp = this.clock.instant().getEpochSecond();
        long timestampDifference;
        try {
            timestampDifference = Math.subtractExact(webhookTimestamp, currentTimestamp);
        } catch (ArithmeticException _) {
            return false;
        }
        if (timestampDifference < -this.timestampToleranceSeconds || timestampDifference > this.timestampToleranceSeconds) {
            return false;
        }

        String expectedSignature = this.signatureComponent.sign(webhookId, webhookTimestamp, bodyBytes);
        byte[] expectedSignatureBytes = Base64.getDecoder().decode(expectedSignature.substring(SIGNATURE_PREFIX.length()));
        for (String candidateSignature : signatureHeader.split(" +")) {
            if (!candidateSignature.startsWith(SIGNATURE_PREFIX)) {
                continue;
            }
            try {
                byte[] candidateSignatureBytes = Base64.getDecoder().decode(candidateSignature.substring(SIGNATURE_PREFIX.length()));
                if (MessageDigest.isEqual(expectedSignatureBytes, candidateSignatureBytes)) {
                    return true;
                }
            } catch (IllegalArgumentException _) {
                // Inspect the remaining signatures.
            }
        }
        return false;
    }
}
