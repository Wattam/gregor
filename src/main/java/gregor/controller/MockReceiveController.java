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

    public MockReceiveController(
            SignatureComponent signatureComponent,
            WebhookProperties webhookProperties,
            Clock clock
    ) {
        if (webhookProperties.receiverTimestampTolerance().isNegative()) {
            throw new IllegalArgumentException("Webhook timestamp tolerance must not be negative");
        }
        this.signatureComponent = signatureComponent;
        this.timestampToleranceSeconds = webhookProperties.receiverTimestampTolerance().toSeconds();
        this.clock = clock;
    }

    @PostMapping
    public ResponseEntity<Void> receive(
            @RequestBody byte[] body,
            @RequestHeader(name = "webhook-id", required = false) String webhookId,
            @RequestHeader(name = "webhook-timestamp", required = false) String timestamp,
            @RequestHeader(name = "webhook-signature", required = false) String signature
    ) {
        if (this.verify(webhookId, timestamp, signature, body)) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.status(401).build();
    }

    private boolean verify(String webhookId, String timestamp, String signatures, byte[] body) {
        if (webhookId == null || timestamp == null || signatures == null || body == null) {
            return false;
        }

        long timestampValue;
        try {
            timestampValue = Long.parseLong(timestamp);
        } catch (NumberFormatException exception) {
            return false;
        }
        long now = this.clock.instant().getEpochSecond();
        long difference;
        try {
            difference = Math.subtractExact(timestampValue, now);
        } catch (ArithmeticException exception) {
            return false;
        }
        if (difference < -this.timestampToleranceSeconds || difference > this.timestampToleranceSeconds) {
            return false;
        }

        String expectedSignature = this.signatureComponent.sign(webhookId, timestampValue, body);
        byte[] expected = Base64.getDecoder().decode(expectedSignature.substring(SIGNATURE_PREFIX.length()));
        for (String signature : signatures.split(" +")) {
            if (!signature.startsWith(SIGNATURE_PREFIX)) {
                continue;
            }
            try {
                byte[] candidate = Base64.getDecoder().decode(signature.substring(SIGNATURE_PREFIX.length()));
                if (MessageDigest.isEqual(expected, candidate)) {
                    return true;
                }
            } catch (IllegalArgumentException exception) {
                // Inspect the remaining signatures.
            }
        }
        return false;
    }
}
