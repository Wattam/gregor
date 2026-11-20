package gregor.component;

import gregor.avro.WebhookEvent;
import gregor.config.properties.WebhookProperties;
import gregor.dto.DeliveryAttemptResult;
import gregor.dto.DeliveryOutcome;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.io.IOException;
import java.net.ProtocolException;
import java.net.URI;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
public class RestComponent {

    private static final int MAXIMUM_RESPONSE_READ_BYTES = 4_096;

    private final RestClient restClient;
    private final SignatureComponent signatureComponent;
    private final Clock clock;
    private final URI receiverUri;

    public RestComponent(RestClient restClient, SignatureComponent signatureComponent, Clock clock, WebhookProperties webhookProperties) {
        this.restClient = restClient;
        this.signatureComponent = signatureComponent;
        this.clock = clock;
        this.receiverUri = webhookProperties.receiverUrl();
    }

    public DeliveryAttemptResult sendWebhook(WebhookEvent webhookEvent) {
        byte[] requestBodyBytes = copyPayload(webhookEvent.getPayload());
        Instant attemptedAt = Instant.now(this.clock);
        long webhookTimestamp = attemptedAt.getEpochSecond();
        String signature = this.signatureComponent.sign(webhookEvent.getEventId(), webhookTimestamp, requestBodyBytes);
        long startNanos = System.nanoTime();
        try {
            int httpStatus = this.restClient
                    .post()
                    .uri(this.receiverUri)
                    .contentType(new MediaType(MediaType.APPLICATION_JSON, StandardCharsets.UTF_8))
                    .header("webhook-event-type", webhookEvent.getEventType())
                    .header("webhook-id", webhookEvent.getEventId())
                    .header("webhook-timestamp", Long.toString(webhookTimestamp))
                    .header("webhook-signature", signature)
                    .body(requestBodyBytes)
                    .exchange((_, convertibleClientHttpResponse) -> {
                        int responseStatus = convertibleClientHttpResponse.getStatusCode().value();
                        // Keep bounded read failures in the transport failure boundary. Discard receiver content.
                        convertibleClientHttpResponse.getBody().readNBytes(MAXIMUM_RESPONSE_READ_BYTES);
                        return responseStatus;
                    });
            Instant completedAt = Instant.now(this.clock);
            DeliveryOutcome deliveryOutcome = classifyStatus(httpStatus);
            Map<String, String> details = deliveryOutcome == DeliveryOutcome.SUCCEEDED ? Map.of() : Map.of("type", "HTTP_FAILURE");

            return new DeliveryAttemptResult(deliveryOutcome, httpStatus, elapsedMilliseconds(startNanos), attemptedAt, completedAt, details);
        } catch (RestClientException restClientException) {
            Instant completedAt = Instant.now(this.clock);
            TransportFailure transportFailure = classifyTransportFailure(restClientException);
            return new DeliveryAttemptResult(DeliveryOutcome.RETRYABLE_FAILURE, null, elapsedMilliseconds(startNanos), attemptedAt, completedAt, transportFailureDetails(transportFailure));
        }
    }

    private static DeliveryOutcome classifyStatus(int httpStatus) {
        if (httpStatus >= 200 && httpStatus < 300) {
            return DeliveryOutcome.SUCCEEDED;
        }
        if (httpStatus == 408 || httpStatus == 429 || httpStatus >= 500) {
            return DeliveryOutcome.RETRYABLE_FAILURE;
        }
        if (httpStatus >= 300) {
            return DeliveryOutcome.PERMANENT_FAILURE;
        }
        return DeliveryOutcome.RETRYABLE_FAILURE;
    }

    private static Map<String, String> transportFailureDetails(TransportFailure transportFailure) {
        Map<String, String> details = new LinkedHashMap<>();
        details.put("type", transportFailure.failureType());
        details.put("exceptionClass", transportFailure.failureThrowable().getClass().getSimpleName());
        String failureMessage = switch (transportFailure.failureType()) {
            case "TIMEOUT" -> "Webhook request timed out.";
            case "INVALID_RESPONSE" -> "Webhook response was invalid.";
            default -> "Webhook connection failed.";
        };
        details.put("message", failureMessage);
        return Map.copyOf(details);
    }

    private static TransportFailure classifyTransportFailure(RestClientException restClientException) {
        IOException transportIOException = null;
        for (Throwable throwable = restClientException; throwable != null; throwable = throwable.getCause()) {
            switch (throwable) {
                case HttpTimeoutException httpTimeoutException -> {
                    return new TransportFailure("TIMEOUT", httpTimeoutException);
                }
                case ProtocolException protocolException -> {
                    return new TransportFailure("INVALID_RESPONSE", protocolException);
                }
                case IOException ioException -> transportIOException = ioException;
                default -> {
                }
            }
        }
        if (transportIOException != null) {
            return new TransportFailure("CONNECTION_FAILURE", transportIOException);
        }
        return new TransportFailure("INVALID_RESPONSE", restClientException);
    }

    private static byte[] copyPayload(ByteBuffer payloadByteBuffer) {
        ByteBuffer duplicateByteBuffer = payloadByteBuffer.duplicate();
        byte[] payloadBytes = new byte[duplicateByteBuffer.remaining()];
        duplicateByteBuffer.get(payloadBytes);
        return payloadBytes;
    }

    private static long elapsedMilliseconds(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    private record TransportFailure(String failureType, Throwable failureThrowable) {}
}
