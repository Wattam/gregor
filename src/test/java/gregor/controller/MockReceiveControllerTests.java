package gregor.controller;

import gregor.component.SignatureComponent;
import gregor.config.WebhookPropertiesFixtures;
import gregor.config.properties.WebhookProperties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;

class MockReceiveControllerTests {

    private static final String SIGNING_SECRET = "whsec_" + Base64.getEncoder().encodeToString(
            "01234567890123456789012345678901".getBytes(StandardCharsets.UTF_8)
    );

    @Test
    void returnsNoContentForAnyMatchingSpaceDelimitedV1SignatureAndTheExactRawBody() throws Exception {
        Instant now = Instant.ofEpochSecond(1_700_000_000L);
        TestContext context = context(now, Duration.ofMinutes(5));
        byte[] body = "{\"orderId\":\"é\"}".getBytes(StandardCharsets.UTF_8);
        String valid = context.signatureComponent().sign("event-123", now.getEpochSecond(), body);
        String wrong = "v1," + Base64.getEncoder().encodeToString(new byte[32]);

        MockHttpServletResponse response = post(
                context.mockMvc(),
                body,
                "event-123",
                Long.toString(now.getEpochSecond()),
                "v2,ignored " + wrong + " malformed " + valid
        );

        Assertions.assertThat(response.getStatus()).isEqualTo(204);
        Assertions.assertThat(response.getContentAsByteArray()).isEmpty();
        Assertions.assertThat(post(
                context.mockMvc(),
                "changed".getBytes(StandardCharsets.UTF_8),
                "event-123",
                Long.toString(now.getEpochSecond()),
                valid
        ).getStatus()).isEqualTo(401);
    }

    @Test
    void acceptsInclusiveTimestampBoundaryAndRejectsStaleOrFutureMetadata() throws Exception {
        Instant now = Instant.ofEpochSecond(10_000L);
        TestContext context = context(now, Duration.ofMinutes(5));
        byte[] body = "{}".getBytes(StandardCharsets.UTF_8);

        Assertions.assertThat(postSigned(context, body, 9_700L).getStatus()).isEqualTo(204);
        Assertions.assertThat(postSigned(context, body, 10_300L).getStatus()).isEqualTo(204);
        Assertions.assertThat(postSigned(context, body, 9_699L).getStatus()).isEqualTo(401);
        Assertions.assertThat(postSigned(context, body, 10_301L).getStatus()).isEqualTo(401);
    }

    @Test
    void returnsAnEmptyUnauthorizedResponseForMissingMalformedOrNonmatchingMetadata() throws Exception {
        Instant now = Instant.ofEpochSecond(1_000L);
        TestContext context = context(now, Duration.ofMinutes(5));
        byte[] body = "{}".getBytes(StandardCharsets.UTF_8);

        MockHttpServletResponse missingHeaders = post(context.mockMvc(), body, null, null, null);
        MockHttpServletResponse malformedTimestamp = post(
                context.mockMvc(), body, "event-123", "not-a-number", "v1,signature"
        );
        MockHttpServletResponse malformedSignature = post(
                context.mockMvc(), body, "event-123", "1000", "v1,not-base64!"
        );
        MockHttpServletResponse wrongEvent = post(
                context.mockMvc(),
                body,
                "different-event",
                "1000",
                context.signatureComponent().sign("event-123", 1_000L, body)
        );

        Assertions.assertThat(missingHeaders.getStatus()).isEqualTo(401);
        Assertions.assertThat(missingHeaders.getContentAsByteArray()).isEmpty();
        Assertions.assertThat(malformedTimestamp.getStatus()).isEqualTo(401);
        Assertions.assertThat(malformedSignature.getStatus()).isEqualTo(401);
        Assertions.assertThat(wrongEvent.getStatus()).isEqualTo(401);
    }

    @Test
    void rejectsNegativeTimestampTolerance() {
        WebhookProperties properties = WebhookPropertiesFixtures.create(SIGNING_SECRET, Duration.ofSeconds(-1));
        SignatureComponent signatureComponent = new SignatureComponent(properties);

        Assertions.assertThatThrownBy(
                () -> new MockReceiveController(signatureComponent, properties, Clock.systemUTC())
        ).isInstanceOf(IllegalArgumentException.class);
    }

    private static MockHttpServletResponse postSigned(TestContext context, byte[] body, long timestamp)
            throws Exception {
        String signature = context.signatureComponent().sign("event-123", timestamp, body);
        return post(context.mockMvc(), body, "event-123", Long.toString(timestamp), signature);
    }

    private static MockHttpServletResponse post(
            MockMvc mockMvc,
            byte[] body,
            String webhookId,
            String timestamp,
            String signature
    ) throws Exception {
        var request = MockMvcRequestBuilders.post("/api/mocked-webhook-receiver")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body);
        if (webhookId != null) {
            request.header("webhook-id", webhookId);
        }
        if (timestamp != null) {
            request.header("webhook-timestamp", timestamp);
        }
        if (signature != null) {
            request.header("webhook-signature", signature);
        }
        return mockMvc.perform(request).andReturn().getResponse();
    }

    private static TestContext context(Instant now, Duration tolerance) {
        WebhookProperties properties = WebhookPropertiesFixtures.create(SIGNING_SECRET, tolerance);
        SignatureComponent signatureComponent = new SignatureComponent(properties);
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);
        MockMvc mockMvc = MockMvcBuilders
                .standaloneSetup(new MockReceiveController(signatureComponent, properties, clock))
                .build();
        return new TestContext(mockMvc, signatureComponent);
    }

    private record TestContext(MockMvc mockMvc, SignatureComponent signatureComponent) {}
}
