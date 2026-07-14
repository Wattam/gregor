package gregor.controller;

import gregor.controller.advice.EventIngestControllerAdvice;
import gregor.dto.EventIngestRequest;
import gregor.exception.EventPublicationException;
import gregor.service.WebhookService;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;
import java.util.UUID;

class EventIngestControllerTests {

    @Test
    void returnsAcceptedEventIdOnlyAfterTheServiceCompletes() throws Exception {
        WebhookService webhookService = Mockito.mock(WebhookService.class);
        UUID eventId = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        Mockito.when(webhookService.ingestEvent(Mockito.any())).thenReturn(eventId);
        MockMvc mockMvc = MockMvcBuilders
                .standaloneSetup(new EventIngestController(webhookService))
                .setControllerAdvice(new EventIngestControllerAdvice())
                .build();

        MockHttpServletResponse response = mockMvc.perform(MockMvcRequestBuilders.post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"eventType\":\"order.created\",\"payload\":{\"orderId\":\"123\"}}"))
                .andReturn()
                .getResponse();

        Assertions.assertThat(response.getStatus()).isEqualTo(202);
        Assertions.assertThat(response.getContentAsString()).contains(eventId.toString());
        ArgumentCaptor<EventIngestRequest> requestCaptor = ArgumentCaptor.forClass(EventIngestRequest.class);
        Mockito.verify(webhookService).ingestEvent(requestCaptor.capture());
        Assertions.assertThat(requestCaptor.getValue().eventType()).isEqualTo("order.created");
        Assertions.assertThat(requestCaptor.getValue().payload().get("orderId").asString()).isEqualTo("123");
    }

    @Test
    void normalizesMalformedAndInvalidRequestsWithoutPublishing() throws Exception {
        WebhookService webhookService = Mockito.mock(WebhookService.class);
        MockMvc mockMvc = MockMvcBuilders
                .standaloneSetup(new EventIngestController(webhookService))
                .setControllerAdvice(new EventIngestControllerAdvice())
                .build();
        List<String> invalidBodies = List.of(
                "{\"eventType\":",
                "{}",
                "{\"eventType\":\"\",\"payload\":{}}",
                "{\"eventType\":\"order.\\r\\ncreated\",\"payload\":{}}",
                "{\"eventType\":\"order.created\"}",
                "{\"eventType\":\"order.created\",\"payload\":null}"
        );

        for (String invalidBody : invalidBodies) {
            MockHttpServletResponse response = mockMvc.perform(MockMvcRequestBuilders.post("/api/events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(invalidBody))
                    .andReturn()
                    .getResponse();
            Assertions.assertThat(response.getStatus()).isEqualTo(400);
            Assertions.assertThat(response.getContentAsString())
                    .contains("\"title\":\"Invalid request\"")
                    .contains("\"detail\":\"Request body is malformed or invalid.\"");
        }

        Mockito.verifyNoInteractions(webhookService);
    }

    @Test
    void normalizesPublicationFailureWithoutReportingAcceptanceOrKafkaDetails() throws Exception {
        WebhookService webhookService = Mockito.mock(WebhookService.class);
        EventPublicationException eventPublicationException = Mockito.mock(EventPublicationException.class);
        Mockito.when(eventPublicationException.getMessage())
                .thenReturn("Kafka broker unavailable at internal-host:9092");
        Mockito.when(webhookService.ingestEvent(Mockito.any())).thenThrow(eventPublicationException);
        MockMvc mockMvc = MockMvcBuilders
                .standaloneSetup(new EventIngestController(webhookService))
                .setControllerAdvice(new EventIngestControllerAdvice())
                .build();

        MockHttpServletResponse response = mockMvc.perform(MockMvcRequestBuilders.post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"eventType\":\"order.created\",\"payload\":true}"))
                .andReturn()
                .getResponse();

        Assertions.assertThat(response.getStatus()).isEqualTo(503);
        Assertions.assertThat(response.getContentAsString())
                .contains("\"title\":\"Event publication unavailable\"")
                .contains("\"detail\":\"The event could not be published.\"")
                .doesNotContain("Kafka");
    }
}
