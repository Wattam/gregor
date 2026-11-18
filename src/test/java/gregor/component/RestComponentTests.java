package gregor.component;

import gregor.avro.WebhookEvent;
import gregor.config.WebhookPropertiesFixtures;
import gregor.dto.DeliveryAttemptResult;
import gregor.dto.DeliveryOutcome;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.AdditionalMatchers;
import org.mockito.Mockito;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.mock.http.client.MockClientHttpRequest;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.test.web.client.match.MockRestRequestMatchers;
import org.springframework.test.web.client.response.MockRestResponseCreators;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.net.ProtocolException;
import java.net.URI;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

class RestComponentTests {

    private static final URI RECEIVER = URI.create("https://receiver.example.test/hooks");
    private static final String SIGNING_SECRET = "whsec_test-signing-secret";

    @Test
    void sendsAndSignsOnlyTheRemainingPayloadBytes() {
        Instant started = Instant.ofEpochSecond(1_700_000_000L);
        Instant completed = started.plusSeconds(2);
        AtomicReference<Instant> now = new AtomicReference<>(started);
        Fixture fixture = fixture(clock(now));
        ByteBuffer payload = ByteBuffer.wrap(new byte[]{9, '{', '"', 'x', '"', ':', '1', '}', 9}, 1, 7);
        byte[] expected = "{\"x\":1}".getBytes(StandardCharsets.UTF_8);
        Mockito.when(fixture.signer().sign(
                Mockito.eq("event-123"), Mockito.eq(started.getEpochSecond()), AdditionalMatchers.aryEq(expected)
        )).thenReturn("v1,exact-body-signature");
        fixture.server().expect(request -> {
            Assertions.assertThat(request.getURI()).isEqualTo(RECEIVER);
            Assertions.assertThat(request.getMethod()).isEqualTo(HttpMethod.POST);
            Assertions.assertThat(((MockClientHttpRequest) request).getBodyAsBytes()).isEqualTo(expected);
            Assertions.assertThat(request.getHeaders().getFirst("webhook-id")).isEqualTo("event-123");
            Assertions.assertThat(request.getHeaders().getFirst("webhook-event-type")).isEqualTo("order.created");
            Assertions.assertThat(request.getHeaders().getFirst("webhook-timestamp")).isEqualTo("1700000000");
            Assertions.assertThat(request.getHeaders().getFirst("webhook-signature"))
                    .isEqualTo("v1,exact-body-signature");
            Assertions.assertThat(request.getHeaders().getContentType())
                    .isEqualTo(new MediaType(MediaType.APPLICATION_JSON, StandardCharsets.UTF_8));
        }).andRespond(request -> {
            now.set(completed);
            return MockRestResponseCreators.withNoContent().createResponse(request);
        });

        DeliveryAttemptResult result = fixture.component().sendWebhook(event(payload));

        fixture.server().verify();
        Assertions.assertThat(result.outcome()).isEqualTo(DeliveryOutcome.SUCCEEDED);
        Assertions.assertThat(result.httpStatus()).isEqualTo(204);
        Assertions.assertThat(result.details()).isEmpty();
        Assertions.assertThat(result.attemptedAt()).isEqualTo(started);
        Assertions.assertThat(result.completedAt()).isEqualTo(completed);
        Assertions.assertThat(payload.position()).isEqualTo(1);
        Assertions.assertThat(payload.limit()).isEqualTo(8);
    }

    @ParameterizedTest
    @CsvSource({
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
    void classifiesHttpStatuses(int status, DeliveryOutcome expected) {
        Fixture fixture = fixture();
        fixture.server().expect(MockRestRequestMatchers.requestTo(RECEIVER))
                .andRespond(MockRestResponseCreators.withRawStatus(status).body("failure"));

        DeliveryAttemptResult result = fixture.component().sendWebhook(event("{}"));

        Assertions.assertThat(result.outcome()).isEqualTo(expected);
        Assertions.assertThat(result.httpStatus()).isEqualTo(status);
        Map<String, String> details = expected == DeliveryOutcome.SUCCEEDED
                ? Map.of()
                : Map.of("type", "HTTP_FAILURE", "responseBody", "failure");
        Assertions.assertThat(result.details()).isEqualTo(details);
    }

    @Test
    void classifiesTransportFailuresAndOmitsRawExceptionMessages() {
        Map<IOException, String> failures = Map.of(
                new HttpTimeoutException("upstream timed out"), "TIMEOUT",
                new ProtocolException("malformed response"), "INVALID_RESPONSE",
                new IOException("Failed https://user:password@example.test/path?token=secret "
                        + "Authorization: Bearer header-secret\n\tat example.Stack.method(Stack.java:1)"),
                "CONNECTION_FAILURE"
        );
        Instant completed = Instant.EPOCH.plusSeconds(2);
        failures.forEach((exception, type) -> {
            AtomicReference<Instant> now = new AtomicReference<>(Instant.EPOCH);
            Fixture fixture = fixture(clock(now));
            fixture.server().expect(MockRestRequestMatchers.requestTo(RECEIVER)).andRespond(_ -> {
                now.set(completed);
                throw exception;
            });

            DeliveryAttemptResult result = fixture.component().sendWebhook(event("{}"));

            Assertions.assertThat(result.outcome())
                    .as("outcome for %s", type)
                    .isEqualTo(DeliveryOutcome.RETRYABLE_FAILURE);
            Assertions.assertThat(result.httpStatus()).isNull();
            Assertions.assertThat(result.attemptedAt()).isEqualTo(Instant.EPOCH);
            Assertions.assertThat(result.completedAt()).isEqualTo(completed);
            Assertions.assertThat(result.details())
                    .containsEntry("type", type)
                    .containsEntry("exceptionClass", exception.getClass().getSimpleName())
                    .containsOnlyKeys("type", "exceptionClass", "message");
            Assertions.assertThat(result.details().get("message"))
                    .doesNotContain(exception.getMessage(), "password", "token=secret", "header-secret", "Stack.java");
        });
    }

    @Test
    void truncatesResponseDetailsAtTheLastCompleteUtf8CodePoint() {
        Fixture fixture = fixture();
        String responseBody = "a" + "🙂".repeat(1_100);
        fixture.server().expect(MockRestRequestMatchers.requestTo(RECEIVER))
                .andRespond(MockRestResponseCreators.withRawStatus(503).body(responseBody));

        DeliveryAttemptResult result = fixture.component().sendWebhook(event("{}"));

        Assertions.assertThat(result.details()).containsEntry("responseBody", "a" + "🙂".repeat(1_023));
    }

    @Test
    void omitsResponseDetailsContainingEitherTheRequestPayloadOrSigningSecret() {
        String payload = "{\"private\":\"payload-marker\"}";
        for (String sensitiveValue : new String[]{payload, SIGNING_SECRET}) {
            Fixture fixture = fixture();
            fixture.server().expect(MockRestRequestMatchers.requestTo(RECEIVER))
                    .andRespond(MockRestResponseCreators.withRawStatus(503).body("Receiver echoed: " + sensitiveValue));

            DeliveryAttemptResult result = fixture.component().sendWebhook(event(payload));

            Assertions.assertThat(result.details()).isEqualTo(Map.of("type", "HTTP_FAILURE"));
        }
    }

    private static Fixture fixture() {
        return fixture(Clock.fixed(Instant.EPOCH, ZoneOffset.UTC));
    }

    private static Fixture fixture(Clock clock) {
        RestClient.Builder builder = RestClient.builder();
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        SignatureComponent signer = Mockito.mock(SignatureComponent.class);
        Mockito.when(signer.sign(Mockito.anyString(), Mockito.anyLong(), Mockito.any())).thenReturn("v1,digest");
        return new Fixture(server, signer, new RestComponent(
                builder.build(), signer, clock, WebhookPropertiesFixtures.create()
        ));
    }

    private static Clock clock(AtomicReference<Instant> now) {
        Clock clock = Mockito.mock(Clock.class);
        Mockito.when(clock.instant()).thenAnswer(_ -> now.get());
        return clock;
    }

    private static WebhookEvent event(String payload) {
        return event(ByteBuffer.wrap(payload.getBytes(StandardCharsets.UTF_8)));
    }

    private static WebhookEvent event(ByteBuffer payload) {
        return new WebhookEvent(
                "event-123", "order.created", "tenant-123", "endpoint-123", Instant.EPOCH, payload, 0, null, 0, 0
        );
    }

    private record Fixture(MockRestServiceServer server, SignatureComponent signer, RestComponent component) {
    }
}
