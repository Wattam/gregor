package gregor.component;

import gregor.avro.WebhookEvent;
import gregor.config.WebhookPropertiesFixtures;
import gregor.dto.DeliveryAttemptResult;
import gregor.dto.DeliveryOutcome;
import org.assertj.core.api.Assertions;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.AdditionalMatchers;
import org.mockito.Mockito;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.mock.http.client.MockClientHttpRequest;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.test.web.client.match.MockRestRequestMatchers;
import org.springframework.test.web.client.response.MockRestResponseCreators;
import org.springframework.web.client.RestClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ProtocolException;
import java.net.URI;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

class RestComponentTests {

    private static final URI TEST_RECEIVER_URI = URI.create("https://receiver.example.test/hooks");

    @Test
    void sendsAndSignsOnlyTheRemainingPayloadBytes() {
        Instant attemptedAt = Instant.ofEpochSecond(1_700_000_000L);
        Instant completedAt = attemptedAt.plusSeconds(2);
        AtomicReference<Instant> currentInstantReference = new AtomicReference<>(attemptedAt);
        Fixture fixture = fixture(clock(currentInstantReference));
        ByteBuffer payloadByteBuffer = ByteBuffer.wrap(new byte[]{9, '{', '"', 'x', '"', ':', '1', '}', 9}, 1, 7);
        byte[] expectedPayloadBytes = "{\"x\":1}".getBytes(StandardCharsets.UTF_8);
        Mockito.when(fixture.signatureComponent().sign(Mockito.eq("event-123"), Mockito.eq(attemptedAt.getEpochSecond()), AdditionalMatchers.aryEq(expectedPayloadBytes))).thenReturn("v1,exact-body-signature");
        fixture.mockRestServiceServer().expect(clientHttpRequest -> {
                    Assertions.assertThat(clientHttpRequest.getURI()).isEqualTo(TEST_RECEIVER_URI);
                    Assertions.assertThat(clientHttpRequest.getMethod()).isEqualTo(HttpMethod.POST);
                    Assertions.assertThat(((MockClientHttpRequest) clientHttpRequest).getBodyAsBytes()).isEqualTo(expectedPayloadBytes);
                    Assertions.assertThat(clientHttpRequest.getHeaders().getFirst("webhook-id")).isEqualTo("event-123");
                    Assertions.assertThat(clientHttpRequest.getHeaders().getFirst("webhook-event-type")).isEqualTo("order.created");
                    Assertions.assertThat(clientHttpRequest.getHeaders().getFirst("webhook-timestamp")).isEqualTo("1700000000");
                    Assertions.assertThat(clientHttpRequest.getHeaders().getFirst("webhook-signature")).isEqualTo("v1,exact-body-signature");
                    Assertions.assertThat(clientHttpRequest.getHeaders().getContentType()).isEqualTo(new MediaType(MediaType.APPLICATION_JSON, StandardCharsets.UTF_8));
                })
                .andRespond(clientHttpRequest -> {
                    currentInstantReference.set(completedAt);
                    return MockRestResponseCreators.withNoContent().createResponse(clientHttpRequest);
                });

        DeliveryAttemptResult deliveryAttemptResult = fixture.restComponent().sendWebhook(event(payloadByteBuffer));

        fixture.mockRestServiceServer().verify();
        Assertions.assertThat(deliveryAttemptResult.outcome()).isEqualTo(DeliveryOutcome.SUCCEEDED);
        Assertions.assertThat(deliveryAttemptResult.httpStatus()).isEqualTo(204);
        Assertions.assertThat(deliveryAttemptResult.details()).isEmpty();
        Assertions.assertThat(deliveryAttemptResult.attemptedAt()).isEqualTo(attemptedAt);
        Assertions.assertThat(deliveryAttemptResult.completedAt()).isEqualTo(completedAt);
        Assertions.assertThat(payloadByteBuffer.position()).isEqualTo(1);
        Assertions.assertThat(payloadByteBuffer.limit()).isEqualTo(8);
    }

    @ParameterizedTest
    @CsvSource({
            "101, RETRYABLE_FAILURE",
            "200, SUCCEEDED",
            "299, SUCCEEDED",
            "301, PERMANENT_FAILURE",
            "400, PERMANENT_FAILURE",
            "408, RETRYABLE_FAILURE",
            "429, RETRYABLE_FAILURE",
            "499, PERMANENT_FAILURE",
            "500, RETRYABLE_FAILURE",
            "599, RETRYABLE_FAILURE"
    })
    void classifiesHttpStatuses(int status, DeliveryOutcome expectedDeliveryOutcome) {
        Fixture fixture = fixture();
        fixture.mockRestServiceServer().expect(MockRestRequestMatchers.requestTo(TEST_RECEIVER_URI)).andRespond(MockRestResponseCreators.withRawStatus(status).body("failure"));

        DeliveryAttemptResult deliveryAttemptResult = fixture.restComponent().sendWebhook(event("{}"));

        Assertions.assertThat(deliveryAttemptResult.outcome()).isEqualTo(expectedDeliveryOutcome);
        Assertions.assertThat(deliveryAttemptResult.httpStatus()).isEqualTo(status);
        Map<String, String> details = Objects.equals(expectedDeliveryOutcome, DeliveryOutcome.SUCCEEDED) ? Map.of() : Map.of("type", "HTTP_FAILURE");
        Assertions.assertThat(deliveryAttemptResult.details()).isEqualTo(details);
    }

    @Test
    void classifiesTransportFailuresAndOmitsRawExceptionMessages() {
        Map<IOException, String> failures = Map.of(
                new HttpTimeoutException("upstream timed out"), "TIMEOUT",
                new ProtocolException("malformed response"), "INVALID_RESPONSE",
                new IOException("Failed https://user:password@example.test/path?token=secret Authorization: Bearer header-secret\n\tat example.Stack.method(Stack.java:1)"), "CONNECTION_FAILURE"
        );
        Instant completedAt = Instant.EPOCH.plusSeconds(2);
        failures.forEach((exception, type) -> {
            AtomicReference<Instant> currentInstantReference = new AtomicReference<>(Instant.EPOCH);
            Fixture fixture = fixture(clock(currentInstantReference));
            fixture.mockRestServiceServer().expect(MockRestRequestMatchers.requestTo(TEST_RECEIVER_URI)).andRespond(_ -> {
                currentInstantReference.set(completedAt);
                throw exception;
            });

            DeliveryAttemptResult deliveryAttemptResult = fixture.restComponent().sendWebhook(event("{}"));

            Assertions.assertThat(deliveryAttemptResult.outcome()).as("outcome for %s", type).isEqualTo(DeliveryOutcome.RETRYABLE_FAILURE);
            Assertions.assertThat(deliveryAttemptResult.httpStatus()).isNull();
            Assertions.assertThat(deliveryAttemptResult.attemptedAt()).isEqualTo(Instant.EPOCH);
            Assertions.assertThat(deliveryAttemptResult.completedAt()).isEqualTo(completedAt);
            Assertions.assertThat(deliveryAttemptResult.details()).containsEntry("type", type).containsEntry("exceptionClass", exception.getClass().getSimpleName()).containsOnlyKeys("type", "exceptionClass", "message");
            Assertions.assertThat(deliveryAttemptResult.details().get("message")).doesNotContain(exception.getMessage(), "password", "token=secret", "header-secret", "Stack.java");
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {
            4_095,
            4_096
    })
    void onlyReadFailuresWithinTheResponseLimitChangeTheHttpOutcome(int bytesBeforeFailure) throws IOException {
        Fixture fixture = fixture();
        byte[] responseBodyBytes = "x".repeat(bytesBeforeFailure).getBytes(StandardCharsets.UTF_8);
        ClientHttpResponse clientHttpResponse = Mockito.mock(ClientHttpResponse.class);
        Mockito.when(clientHttpResponse.getStatusCode()).thenReturn(HttpStatus.BAD_REQUEST);
        Mockito.when(clientHttpResponse.getBody()).thenReturn(new InputStream() {
            private final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(responseBodyBytes);

            @Override
            public int read() throws IOException {
                return requireByte(this.byteArrayInputStream.read());
            }

            @Override
            public int read(byte @NonNull [] bytes, int offset, int length) throws IOException {
                return length == 0 ? 0 : requireByte(this.byteArrayInputStream.read(bytes, offset, length));
            }

            private static int requireByte(int result) throws IOException {
                if (result < 0) {
                    throw new IOException("Receiver stopped sending the body");
                }
                return result;
            }
        });
        fixture.mockRestServiceServer().expect(MockRestRequestMatchers.requestTo(TEST_RECEIVER_URI)).andRespond(_ -> clientHttpResponse);

        DeliveryAttemptResult deliveryAttemptResult = fixture.restComponent().sendWebhook(event("{}"));

        if (bytesBeforeFailure == 4_096) {
            Assertions.assertThat(deliveryAttemptResult.outcome()).isEqualTo(DeliveryOutcome.PERMANENT_FAILURE);
            Assertions.assertThat(deliveryAttemptResult.httpStatus()).isEqualTo(400);
            Assertions.assertThat(deliveryAttemptResult.details()).isEqualTo(Map.of("type", "HTTP_FAILURE"));
        } else {
            Assertions.assertThat(deliveryAttemptResult.outcome()).isEqualTo(DeliveryOutcome.RETRYABLE_FAILURE);
            Assertions.assertThat(deliveryAttemptResult.httpStatus()).isNull();
            Assertions.assertThat(deliveryAttemptResult.details()).containsEntry("type", "CONNECTION_FAILURE");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "Unknown user: 42",
            "Rejected: {\"error\":\"\\u0077hsec_test-signing-secret\"}"
    })
    void sendsOpaquePayloadBytesAndOmitsReceiverContentOnHttpFailure(String receiverText) {
        Fixture fixture = fixture();
        byte[] payloadBytes = {'i', 'd', '=', '4', '2', (byte) 0xFF};
        fixture.mockRestServiceServer()
                .expect(MockRestRequestMatchers.requestTo(TEST_RECEIVER_URI))
                .andExpect(MockRestRequestMatchers.content().bytes(payloadBytes))
                .andRespond(MockRestResponseCreators.withRawStatus(400).body(receiverText));

        DeliveryAttemptResult deliveryAttemptResult = fixture.restComponent().sendWebhook(event(ByteBuffer.wrap(payloadBytes)));

        fixture.mockRestServiceServer().verify();
        Assertions.assertThat(deliveryAttemptResult.outcome()).isEqualTo(DeliveryOutcome.PERMANENT_FAILURE);
        Assertions.assertThat(deliveryAttemptResult.httpStatus()).isEqualTo(400);
        Assertions.assertThat(deliveryAttemptResult.details()).isEqualTo(Map.of("type", "HTTP_FAILURE"));
    }

    private static Fixture fixture() {
        return fixture(Clock.fixed(Instant.EPOCH, ZoneOffset.UTC));
    }

    private static Fixture fixture(Clock clock) {
        RestClient.Builder restClientBuilder = RestClient.builder();
        MockRestServiceServer mockRestServiceServer = MockRestServiceServer.bindTo(restClientBuilder).build();
        SignatureComponent signatureComponent = Mockito.mock(SignatureComponent.class);
        Mockito.when(signatureComponent.sign(Mockito.anyString(), Mockito.anyLong(), Mockito.any())).thenReturn("v1,digest");
        RestComponent restComponent = new RestComponent(restClientBuilder.build(), signatureComponent, clock, WebhookPropertiesFixtures.create());
        return new Fixture(mockRestServiceServer, signatureComponent, restComponent);
    }

    private static Clock clock(AtomicReference<Instant> currentInstantReference) {
        Clock clock = Mockito.mock(Clock.class);
        Mockito.when(clock.instant()).thenAnswer(_ -> currentInstantReference.get());
        return clock;
    }

    private static WebhookEvent event(String payload) {
        return event(ByteBuffer.wrap(payload.getBytes(StandardCharsets.UTF_8)));
    }

    private static WebhookEvent event(ByteBuffer payloadByteBuffer) {
        return new WebhookEvent("event-123", "order.created", "tenant-123", "endpoint-123", Instant.EPOCH, payloadByteBuffer, 0, null, 0, 0);
    }

    private record Fixture(MockRestServiceServer mockRestServiceServer, SignatureComponent signatureComponent, RestComponent restComponent) {
    }
}
