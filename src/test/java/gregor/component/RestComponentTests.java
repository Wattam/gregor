package gregor.component;

import gregor.avro.WebhookEvent;
import gregor.config.WebhookPropertiesFixtures;
import gregor.dto.DeliveryAttemptResult;
import gregor.dto.DeliveryOutcome;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.AdditionalMatchers;
import org.mockito.Mockito;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.mock.http.client.MockClientHttpRequest;
import org.springframework.test.web.client.MockRestServiceServer;
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
import java.util.LinkedHashMap;
import java.util.Map;

class RestComponentTests {

    private static final String SIGNING_SECRET = "whsec_test-signing-secret";

    @Test
    void sendsTheExactPayloadWithRequiredHeadersAndSignature() {
        RestClient.Builder builder = RestClient.builder();
        MockRestServiceServer mockRestServiceServer = MockRestServiceServer.bindTo(builder).build();
        SignatureComponent signatureComponent = Mockito.mock(SignatureComponent.class);
        Clock clock = Clock.fixed(Instant.ofEpochSecond(1_700_000_000L), ZoneOffset.UTC);
        RestComponent restComponent = new RestComponent(
                builder.build(),
                signatureComponent,
                clock,
                WebhookPropertiesFixtures.create()
        );
        byte[] backingPayload = new byte[]{9, '{', '"', 'x', '"', ':', '1', '}', 9};
        ByteBuffer payload = ByteBuffer.wrap(backingPayload);
        payload.position(1);
        payload.limit(8);
        WebhookEvent webhookEvent = new WebhookEvent(
                "event-123",
                "order.created",
                "tenant-123",
                "endpoint-123",
                Instant.EPOCH,
                payload,
                0
        );
        byte[] expectedBody = "{\"x\":1}".getBytes(StandardCharsets.UTF_8);
        Mockito.when(signatureComponent.sign(
                Mockito.eq("event-123"),
                Mockito.eq(1_700_000_000L),
                AdditionalMatchers.aryEq(expectedBody)
        )).thenReturn("v1,digest");
        mockRestServiceServer.expect(request -> {
            MockClientHttpRequest mockRequest = (MockClientHttpRequest) request;
            Assertions.assertThat(request.getURI()).isEqualTo(URI.create("https://receiver.example.test/hooks"));
            Assertions.assertThat(request.getMethod()).isEqualTo(HttpMethod.POST);
            Assertions.assertThat(mockRequest.getBodyAsBytes()).isEqualTo(expectedBody);
            Assertions.assertThat(request.getHeaders().getFirst("webhook-event-type")).isEqualTo("order.created");
            Assertions.assertThat(request.getHeaders().getFirst("webhook-id")).isEqualTo("event-123");
            Assertions.assertThat(request.getHeaders().getFirst("webhook-timestamp")).isEqualTo("1700000000");
            Assertions.assertThat(request.getHeaders().getFirst("webhook-signature")).isEqualTo("v1,digest");
            Assertions.assertThat(request.getHeaders().getContentType())
                    .isEqualTo(new MediaType(MediaType.APPLICATION_JSON, StandardCharsets.UTF_8));
        }).andRespond(MockRestResponseCreators.withNoContent());

        DeliveryAttemptResult deliveryAttemptResult = restComponent.sendWebhook(webhookEvent);
        Assertions.assertThat(deliveryAttemptResult.outcome()).isEqualTo(DeliveryOutcome.SUCCEEDED);
        Assertions.assertThat(deliveryAttemptResult.httpStatus()).isEqualTo(204);
        Assertions.assertThat(deliveryAttemptResult.latencyMs()).isNotNegative();
        Assertions.assertThat(deliveryAttemptResult.attemptedAt()).isEqualTo(Instant.ofEpochSecond(1_700_000_000L));
        Assertions.assertThat(deliveryAttemptResult.details()).isEmpty();
        Assertions.assertThat(payload.position()).isEqualTo(1);
        Assertions.assertThat(payload.limit()).isEqualTo(8);
        Mockito.verify(signatureComponent).sign(
                Mockito.eq("event-123"),
                Mockito.eq(1_700_000_000L),
                AdditionalMatchers.aryEq(expectedBody)
        );
    }

    @Test
    void classifiesEveryHttpStatusFamilyAndSpecialRetryableStatus() {
        Map<Integer, DeliveryOutcome> cases = new LinkedHashMap<>();
        cases.put(200, DeliveryOutcome.SUCCEEDED);
        cases.put(299, DeliveryOutcome.SUCCEEDED);
        cases.put(301, DeliveryOutcome.PERMANENT_FAILURE);
        cases.put(400, DeliveryOutcome.PERMANENT_FAILURE);
        cases.put(408, DeliveryOutcome.RETRYABLE_FAILURE);
        cases.put(429, DeliveryOutcome.RETRYABLE_FAILURE);
        cases.put(499, DeliveryOutcome.PERMANENT_FAILURE);
        cases.put(500, DeliveryOutcome.RETRYABLE_FAILURE);
        cases.put(599, DeliveryOutcome.RETRYABLE_FAILURE);

        for (Map.Entry<Integer, DeliveryOutcome> testCase : cases.entrySet()) {
            RestClient.Builder builder = RestClient.builder();
            MockRestServiceServer mockRestServiceServer = MockRestServiceServer.bindTo(builder).build();
            SignatureComponent signatureComponent = Mockito.mock(SignatureComponent.class);
            Mockito.when(signatureComponent.sign(Mockito.anyString(), Mockito.anyLong(), Mockito.any()))
                    .thenReturn("v1,digest");
            RestComponent restComponent = new RestComponent(
                    builder.build(),
                    signatureComponent,
                    Clock.fixed(Instant.EPOCH, ZoneOffset.UTC),
                    WebhookPropertiesFixtures.create()
            );
            WebhookEvent webhookEvent = new WebhookEvent(
                    "event-123",
                    "order.created",
                    "tenant-123",
                    "endpoint-123",
                    Instant.EPOCH,
                    ByteBuffer.wrap("{}".getBytes(StandardCharsets.UTF_8)),
                    0
            );
            mockRestServiceServer
                    .expect(request -> Assertions.assertThat(request.getURI()).isEqualTo(
                            URI.create("https://receiver.example.test/hooks")
                    ))
                    .andRespond(MockRestResponseCreators.withRawStatus(testCase.getKey()).body("failure"));

            DeliveryAttemptResult deliveryAttemptResult = restComponent.sendWebhook(webhookEvent);

            Assertions.assertThat(deliveryAttemptResult.outcome()).isEqualTo(testCase.getValue());
            Assertions.assertThat(deliveryAttemptResult.httpStatus()).isEqualTo(testCase.getKey());
            if (testCase.getValue() == DeliveryOutcome.SUCCEEDED) {
                Assertions.assertThat(deliveryAttemptResult.details()).isEmpty();
            } else {
                Assertions.assertThat(deliveryAttemptResult.details()).containsEntry("type", "HTTP_FAILURE");
                Assertions.assertThat(deliveryAttemptResult.details()).containsEntry("responseBody", "failure");
            }
        }
    }

    @Test
    void classifiesTimeoutProtocolAndConnectionFailuresWithSanitizedDetails() {
        Map<IOException, String> cases = new LinkedHashMap<>();
        cases.put(new HttpTimeoutException("timed out"), "TIMEOUT");
        cases.put(new ProtocolException("malformed response"), "INVALID_RESPONSE");
        cases.put(
                new IOException("Failed https://user:password@example.test/path?token=secret "
                        + "Authorization: Bearer header-secret\n\tat example.Stack.method(Stack.java:1)"),
                "CONNECTION_FAILURE"
        );

        for (Map.Entry<IOException, String> testCase : cases.entrySet()) {
            RestClient.Builder builder = RestClient.builder();
            MockRestServiceServer mockRestServiceServer = MockRestServiceServer.bindTo(builder).build();
            SignatureComponent signatureComponent = Mockito.mock(SignatureComponent.class);
            Mockito.when(signatureComponent.sign(Mockito.anyString(), Mockito.anyLong(), Mockito.any()))
                    .thenReturn("v1,digest");
            RestComponent restComponent = new RestComponent(
                    builder.build(),
                    signatureComponent,
                    Clock.fixed(Instant.EPOCH, ZoneOffset.UTC),
                    WebhookPropertiesFixtures.create()
            );
            WebhookEvent webhookEvent = new WebhookEvent(
                    "event-123",
                    "order.created",
                    "tenant-123",
                    "endpoint-123",
                    Instant.EPOCH,
                    ByteBuffer.wrap("{}".getBytes(StandardCharsets.UTF_8)),
                    0
            );
            mockRestServiceServer
                    .expect(request -> Assertions.assertThat(request.getMethod()).isEqualTo(HttpMethod.POST))
                    .andRespond(MockRestResponseCreators.withException(testCase.getKey()));

            DeliveryAttemptResult deliveryAttemptResult = restComponent.sendWebhook(webhookEvent);

            Assertions.assertThat(deliveryAttemptResult.outcome()).isEqualTo(DeliveryOutcome.RETRYABLE_FAILURE);
            Assertions.assertThat(deliveryAttemptResult.httpStatus()).isNull();
            Assertions.assertThat(deliveryAttemptResult.details()).containsEntry("type", testCase.getValue());
            Assertions.assertThat(deliveryAttemptResult.details())
                    .containsEntry("exceptionClass", testCase.getKey().getClass().getSimpleName());
            Assertions.assertThat(deliveryAttemptResult.details())
                    .containsOnlyKeys("type", "exceptionClass", "message");
            Assertions.assertThat(deliveryAttemptResult.details().get("message")).doesNotContain(
                    "password",
                    "token=secret",
                    "user:password",
                    "Authorization",
                    "header-secret",
                    "Stack.java",
                    "\tat "
            );
        }
    }

    @Test
    void truncatesResponseDetailsWithoutSplittingUtf8CodePoints() {
        RestClient.Builder builder = RestClient.builder();
        MockRestServiceServer mockRestServiceServer = MockRestServiceServer.bindTo(builder).build();
        SignatureComponent signatureComponent = Mockito.mock(SignatureComponent.class);
        Mockito.when(signatureComponent.sign(Mockito.anyString(), Mockito.anyLong(), Mockito.any()))
                .thenReturn("v1,digest");
        RestComponent restComponent = new RestComponent(
                builder.build(),
                signatureComponent,
                Clock.fixed(Instant.EPOCH, ZoneOffset.UTC),
                WebhookPropertiesFixtures.create()
        );
        WebhookEvent webhookEvent = new WebhookEvent(
                "event-123",
                "order.created",
                "tenant-123",
                "endpoint-123",
                Instant.EPOCH,
                ByteBuffer.wrap("{}".getBytes(StandardCharsets.UTF_8)),
                0
        );
        String responseBody = "a" + "🙂".repeat(1_100);
        mockRestServiceServer
                .expect(request -> Assertions.assertThat(request.getMethod()).isEqualTo(HttpMethod.POST))
                .andRespond(MockRestResponseCreators.withStatus(HttpStatusCode.valueOf(503)).body(responseBody));

        DeliveryAttemptResult deliveryAttemptResult = restComponent.sendWebhook(webhookEvent);

        String snippet = deliveryAttemptResult.details().get("responseBody");
        Assertions.assertThat(snippet.getBytes(StandardCharsets.UTF_8).length).isLessThanOrEqualTo(4_096);
        Assertions.assertThat(snippet).doesNotContain("�");
        Assertions.assertThat(responseBody).startsWith(snippet);
    }

    @Test
    void omitsResponseDetailsContainingTheRequestPayloadOrSigningSecret() {
        byte[] payload = "{\"private\":\"payload-marker\"}".getBytes(StandardCharsets.UTF_8);
        for (String responseBody : new String[]{new String(payload, StandardCharsets.UTF_8), SIGNING_SECRET}) {
            RestClient.Builder builder = RestClient.builder();
            MockRestServiceServer mockRestServiceServer = MockRestServiceServer.bindTo(builder).build();
            SignatureComponent signatureComponent = Mockito.mock(SignatureComponent.class);
            Mockito.when(signatureComponent.sign(Mockito.anyString(), Mockito.anyLong(), Mockito.any()))
                    .thenReturn("v1,digest");
            RestComponent restComponent = new RestComponent(
                    builder.build(),
                    signatureComponent,
                    Clock.fixed(Instant.EPOCH, ZoneOffset.UTC),
                    WebhookPropertiesFixtures.create()
            );
            WebhookEvent webhookEvent = new WebhookEvent(
                    "event-123",
                    "order.created",
                    "tenant-123",
                    "endpoint-123",
                    Instant.EPOCH,
                    ByteBuffer.wrap(payload),
                    0
            );
            mockRestServiceServer
                    .expect(request -> Assertions.assertThat(request.getMethod()).isEqualTo(HttpMethod.POST))
                    .andRespond(MockRestResponseCreators.withStatus(HttpStatusCode.valueOf(503)).body(responseBody));

            DeliveryAttemptResult deliveryAttemptResult = restComponent.sendWebhook(webhookEvent);

            Assertions.assertThat(deliveryAttemptResult.details()).containsExactly(Map.entry("type", "HTTP_FAILURE"));
        }
    }
}
