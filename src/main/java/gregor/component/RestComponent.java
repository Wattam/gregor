package gregor.component;

import gregor.avro.WebhookEvent;
import gregor.config.properties.WebhookProperties;
import gregor.dto.DeliveryAttemptResult;
import gregor.dto.DeliveryOutcome;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.io.IOException;
import java.net.ProtocolException;
import java.net.URI;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
public class RestComponent {

    private static final int MAXIMUM_RESPONSE_BODY_BYTES = 4_096;

    private final RestClient restClient;
    private final SignatureComponent signatureComponent;
    private final Clock clock;
    private final URI receiverUri;
    private final String signingSecret;

    public RestComponent(RestClient restClient, SignatureComponent signatureComponent, Clock clock, WebhookProperties webhookProperties) {
        this.restClient = restClient;
        this.signatureComponent = signatureComponent;
        this.clock = clock;
        this.receiverUri = webhookProperties.receiverUrl();
        this.signingSecret = webhookProperties.signingSecret();
    }

    public DeliveryAttemptResult sendWebhook(WebhookEvent webhookEvent) {
        byte[] requestBodyBytes = copyPayload(webhookEvent.getPayload());
        Instant attemptedAt = Instant.now(this.clock);
        long webhookTimestamp = attemptedAt.getEpochSecond();
        String signature = this.signatureComponent.sign(webhookEvent.getEventId(), webhookTimestamp, requestBodyBytes);
        long startNanos = System.nanoTime();
        try {
            HttpAttemptResponse httpAttemptResponse = this.restClient
                    .post()
                    .uri(this.receiverUri)
                    .contentType(new MediaType(MediaType.APPLICATION_JSON, StandardCharsets.UTF_8))
                    .header("webhook-event-type", webhookEvent.getEventType())
                    .header("webhook-id", webhookEvent.getEventId())
                    .header("webhook-timestamp", Long.toString(webhookTimestamp))
                    .header("webhook-signature", signature)
                    .body(requestBodyBytes)
                    .exchange((_, convertibleClientHttpResponse) -> new HttpAttemptResponse(
                            convertibleClientHttpResponse.getStatusCode().value(),
                            responseSnippet(convertibleClientHttpResponse.getBody().readNBytes(MAXIMUM_RESPONSE_BODY_BYTES))
                    ));
            Instant completedAt = Instant.now(this.clock);
            DeliveryOutcome deliveryOutcome = classifyStatus(httpAttemptResponse.httpStatus());
            Map<String, String> details = deliveryOutcome == DeliveryOutcome.SUCCEEDED ? Map.of() : failureDetails(httpAttemptResponse.responseBody(), requestBodyBytes, this.signingSecret);

            return new DeliveryAttemptResult(deliveryOutcome, httpAttemptResponse.httpStatus(), elapsedMilliseconds(startNanos), attemptedAt, completedAt, details);
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

    private static Map<String, String> failureDetails(String responseBody, byte[] requestBodyBytes, String signingSecret) {
        if (StringUtils.isEmpty(responseBody)) {
            return Map.of("type", "HTTP_FAILURE");
        }
        String requestPayload = new String(requestBodyBytes, StandardCharsets.UTF_8);
        if ((!requestPayload.isEmpty() && responseBody.contains(requestPayload)) || responseBody.contains(signingSecret)) {
            return Map.of("type", "HTTP_FAILURE");
        }
        return Map.of("type", "HTTP_FAILURE", "responseBody", responseBody);
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

    private static String responseSnippet(byte[] responseBytes) {
        if (responseBytes.length == 0) {
            return null;
        }
        ByteBuffer responseByteBuffer = ByteBuffer.wrap(responseBytes);
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).decode(responseByteBuffer).toString();
        } catch (CharacterCodingException _) {
            int validByteCount = responseByteBuffer.position();
            if (validByteCount == 0) {
                return null;
            }
            return new String(responseBytes, 0, validByteCount, StandardCharsets.UTF_8);
        }
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

    private record HttpAttemptResponse(int httpStatus, String responseBody) {}

    private record TransportFailure(String failureType, Throwable failureThrowable) {}
}
