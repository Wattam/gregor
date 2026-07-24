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

    public RestComponent(
            RestClient restClient,
            SignatureComponent signatureComponent,
            Clock clock,
            WebhookProperties properties
    ) {
        this.restClient = restClient;
        this.signatureComponent = signatureComponent;
        this.clock = clock;
        this.receiverUri = properties.receiverUrl();
        this.signingSecret = properties.signingSecret();
    }

    public DeliveryAttemptResult sendWebhook(WebhookEvent webhookEvent) {
        byte[] body = copyPayload(webhookEvent.getPayload());
        Instant attemptedAt = Instant.now(this.clock);
        long timestamp = attemptedAt.getEpochSecond();
        String signature = this.signatureComponent.sign(webhookEvent.getEventId(), timestamp, body);
        long startNanos = System.nanoTime();
        try {
            HttpAttemptResponse httpAttemptResponse = this.restClient.post()
                    .uri(this.receiverUri)
                    .contentType(new MediaType(MediaType.APPLICATION_JSON, StandardCharsets.UTF_8))
                    .header("webhook-event-type", webhookEvent.getEventType())
                    .header("webhook-id", webhookEvent.getEventId())
                    .header("webhook-timestamp", Long.toString(timestamp))
                    .header("webhook-signature", signature)
                    .body(body)
                    .exchange((_, clientResponse) -> new HttpAttemptResponse(
                            clientResponse.getStatusCode().value(),
                            responseSnippet(clientResponse.getBody().readNBytes(MAXIMUM_RESPONSE_BODY_BYTES))
                    ));
            Instant completedAt = Instant.now(this.clock);
            DeliveryOutcome deliveryOutcome = classifyStatus(httpAttemptResponse.status());
            Map<String, String> details = deliveryOutcome == DeliveryOutcome.SUCCEEDED
                    ? Map.of()
                    : failureDetails(httpAttemptResponse.responseBody(), body, this.signingSecret);

            return new DeliveryAttemptResult(
                    deliveryOutcome,
                    httpAttemptResponse.status(),
                    elapsedMilliseconds(startNanos),
                    attemptedAt,
                    completedAt,
                    details
            );
        } catch (RestClientException exception) {
            Instant completedAt = Instant.now(this.clock);
            TransportFailure transportFailure = classifyTransportFailure(exception);
            return new DeliveryAttemptResult(
                    DeliveryOutcome.RETRYABLE_FAILURE,
                    null,
                    elapsedMilliseconds(startNanos),
                    attemptedAt,
                    completedAt,
                    transportFailureDetails(transportFailure)
            );
        }
    }

    private static DeliveryOutcome classifyStatus(int status) {
        if (status >= 200 && status < 300) {
            return DeliveryOutcome.SUCCEEDED;
        }
        if (status == 408 || status == 429 || status >= 500) {
            return DeliveryOutcome.RETRYABLE_FAILURE;
        }
        if (status >= 300) {
            return DeliveryOutcome.PERMANENT_FAILURE;
        }
        return DeliveryOutcome.RETRYABLE_FAILURE;
    }

    private static Map<String, String> failureDetails(String responseBody, byte[] requestBody, String signingSecret) {
        if (StringUtils.isEmpty(responseBody)) {
            return Map.of("type", "HTTP_FAILURE");
        }
        String requestPayload = new String(requestBody, StandardCharsets.UTF_8);
        if ((!requestPayload.isEmpty() && responseBody.contains(requestPayload))
                || responseBody.contains(signingSecret)) {
            return Map.of("type", "HTTP_FAILURE");
        }
        return Map.of("type", "HTTP_FAILURE", "responseBody", responseBody);
    }

    private static Map<String, String> transportFailureDetails(TransportFailure transportFailure) {
        Map<String, String> details = new LinkedHashMap<>();
        details.put("type", transportFailure.type());
        details.put("exceptionClass", transportFailure.exception().getClass().getSimpleName());
        details.put("message", switch (transportFailure.type()) {
            case "TIMEOUT" -> "Webhook request timed out.";
            case "INVALID_RESPONSE" -> "Webhook response was invalid.";
            default -> "Webhook connection failed.";
        });
        return Map.copyOf(details);
    }

    private static TransportFailure classifyTransportFailure(RestClientException restClientException) {
        Throwable current = restClientException;
        IOException ioFailure = null;
        while (current != null) {
            switch (current) {
                case HttpTimeoutException _ -> {
                    return new TransportFailure("TIMEOUT", current);
                }
                case ProtocolException _ -> {
                    return new TransportFailure("INVALID_RESPONSE", current);
                }
                case IOException ioException -> ioFailure = ioException;
                default -> {
                }
            }
            current = current.getCause();
        }
        if (ioFailure != null) {
            return new TransportFailure("CONNECTION_FAILURE", ioFailure);
        }
        return new TransportFailure("INVALID_RESPONSE", restClientException);
    }

    private static String responseSnippet(byte[] responseBytes) {
        if (responseBytes.length == 0) {
            return null;
        }
        ByteBuffer input = ByteBuffer.wrap(responseBytes);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(input)
                    .toString();
        } catch (CharacterCodingException exception) {
            int validLength = input.position();
            if (validLength == 0) {
                return null;
            }
            return new String(responseBytes, 0, validLength, StandardCharsets.UTF_8);
        }
    }

    private static byte[] copyPayload(ByteBuffer source) {
        ByteBuffer copy = source.duplicate();
        byte[] body = new byte[copy.remaining()];
        copy.get(body);
        return body;
    }

    private static long elapsedMilliseconds(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    private record HttpAttemptResponse(int status, String responseBody) {}

    private record TransportFailure(String type, Throwable exception) {}
}
