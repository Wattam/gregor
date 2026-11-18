package gregor.integration;

import org.apache.camel.ProducerTemplate;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "gregor.webhook.signing-secret=whsec_MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE="
)
class EventIngestPublicationFailureIntegrationTests {

    @LocalServerPort
    private int serverPort;

    @MockitoBean
    private ProducerTemplate producerTemplate;

    @Test
    void returnsServiceUnavailableWithoutReportingAcceptanceWhenKafkaPublicationFails() throws Exception {
        Mockito.doThrow(new IllegalStateException("broker-internal:9092 unavailable"))
                .when(this.producerTemplate)
                .sendBodyAndHeader(Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.any());
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + this.serverPort + "/api/events"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"eventType\":\"order.created\",\"payload\":true}",
                        StandardCharsets.UTF_8
                ))
                .build();

        HttpResponse<String> response = HttpClient.newHttpClient()
                .send(request, HttpResponse.BodyHandlers.ofString());

        Assertions.assertThat(response.statusCode()).isEqualTo(503);
        Assertions.assertThat(response.body())
                .contains("\"title\":\"Event publication unavailable\"")
                .contains("\"detail\":\"The event could not be published.\"")
                .doesNotContain("eventId", "Kafka", "broker-internal");
    }
}
