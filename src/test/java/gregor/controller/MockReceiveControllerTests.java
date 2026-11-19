package gregor.controller;

import gregor.component.SignatureComponent;
import gregor.config.WebhookPropertiesFixtures;
import gregor.config.properties.WebhookProperties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;

class MockReceiveControllerTests {

    private static final String SIGNING_SECRET = "whsec_" + Base64.getEncoder().encodeToString("01234567890123456789012345678901".getBytes(StandardCharsets.UTF_8));

    @Test
    void returnsNoContentForAnyMatchingSpaceDelimitedV1SignatureAndTheExactRawBody() throws Exception {
        Instant now = Instant.ofEpochSecond(1_700_000_000L);
        MockMvc mockMvc = receiverMockMvc(now);
        byte[] bodyBytes = "{\"orderId\":\"é\"}".getBytes(StandardCharsets.UTF_8);
        String validSignature = "v1,wz8WZnnUX/q+zBNdAgNzhaFx30HjxssuMIHpQ7HGJUg=";
        String wrongSignature = "v1," + Base64.getEncoder().encodeToString(new byte[32]);

        MockHttpServletResponse mockHttpServletResponse = post(mockMvc, bodyBytes, "event-123", "1700000000", "v2,ignored " + wrongSignature + " malformed " + validSignature);

        Assertions.assertThat(mockHttpServletResponse.getStatus()).isEqualTo(204);
        Assertions.assertThat(mockHttpServletResponse.getContentAsByteArray()).isEmpty();
        Assertions.assertThat(post(mockMvc, "changed".getBytes(StandardCharsets.UTF_8), "event-123", "1700000000", validSignature).getStatus()).isEqualTo(401);
    }

    @ParameterizedTest
    @CsvSource(
            delimiter = ';',
            value = {
                    "9700; v1,frGBmgvLTg1Jhj+uyNMk+nL0i3c62WLsp1ubrnt/F/k=; 204",
                    "10300; v1,O3x8IY5aV6ufrCJ9bwSJ5mliiGXS1Ss+ch/f19qhn3I=; 204",
                    "9699; v1,kCxl03KBaRYtieFBHfYxo/JFy1EnPgvX3nEZ8jTwjis=; 401",
                    "10301; v1,57M8GjKjTlVx1aEr2Z86344uI4Mdq+mTc11Vz35p+z0=; 401"
            }
    )
    void acceptsInclusiveTimestampBoundaryAndRejectsStaleOrFutureMetadata(String timestamp, String signature, int expectedStatus) throws Exception {
        MockMvc mockMvc = receiverMockMvc(Instant.ofEpochSecond(10_000L));
        byte[] bodyBytes = "{}".getBytes(StandardCharsets.UTF_8);

        Assertions.assertThat(post(mockMvc, bodyBytes, "event-123", timestamp, signature).getStatus()).isEqualTo(expectedStatus);
    }

    @Test
    void returnsAnEmptyUnauthorizedResponseForMissingMalformedOrNonmatchingMetadata() throws Exception {
        MockMvc mockMvc = receiverMockMvc(Instant.ofEpochSecond(1_000L));
        byte[] bodyBytes = "{}".getBytes(StandardCharsets.UTF_8);

        MockHttpServletResponse missingHeadersResponse = post(mockMvc, bodyBytes, null, null, null);
        MockHttpServletResponse malformedTimestampResponse = post(mockMvc, bodyBytes, "event-123", "not-a-number", "v1,signature");
        MockHttpServletResponse malformedSignatureResponse = post(mockMvc, bodyBytes, "event-123", "1000", "v1,not-base64!");
        MockHttpServletResponse wrongEventResponse = post(mockMvc, bodyBytes, "different-event", "1000", "v1,z8GHfVsj3g1KawkXIgxJQG6bopdUJtaKNDriAftHrTc=");

        Assertions.assertThat(missingHeadersResponse.getStatus()).isEqualTo(401);
        Assertions.assertThat(missingHeadersResponse.getContentAsByteArray()).isEmpty();
        Assertions.assertThat(malformedTimestampResponse.getStatus()).isEqualTo(401);
        Assertions.assertThat(malformedSignatureResponse.getStatus()).isEqualTo(401);
        Assertions.assertThat(wrongEventResponse.getStatus()).isEqualTo(401);
    }

    @Test
    void rejectsNegativeTimestampTolerance() {
        WebhookProperties webhookProperties = WebhookPropertiesFixtures.create(SIGNING_SECRET, Duration.ofSeconds(-1));
        SignatureComponent signatureComponent = new SignatureComponent(webhookProperties);

        Assertions.assertThatThrownBy(() -> new MockReceiveController(signatureComponent, webhookProperties, Clock.systemUTC())).isInstanceOf(IllegalArgumentException.class);
    }

    private static MockHttpServletResponse post(MockMvc mockMvc, byte[] bodyBytes, String webhookId, String timestamp, String signature) throws Exception {
        MockHttpServletRequestBuilder mockHttpServletRequestBuilder = MockMvcRequestBuilders.post("/api/mocked-webhook-receiver").contentType(MediaType.APPLICATION_JSON).content(bodyBytes);
        if (webhookId != null) {
            mockHttpServletRequestBuilder.header("webhook-id", webhookId);
        }
        if (timestamp != null) {
            mockHttpServletRequestBuilder.header("webhook-timestamp", timestamp);
        }
        if (signature != null) {
            mockHttpServletRequestBuilder.header("webhook-signature", signature);
        }
        return mockMvc.perform(mockHttpServletRequestBuilder).andReturn().getResponse();
    }

    private static MockMvc receiverMockMvc(Instant now) {
        WebhookProperties webhookProperties = WebhookPropertiesFixtures.create(SIGNING_SECRET, Duration.ofMinutes(5));
        SignatureComponent signatureComponent = new SignatureComponent(webhookProperties);
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);
        return MockMvcBuilders.standaloneSetup(new MockReceiveController(signatureComponent, webhookProperties, clock)).build();
    }
}
