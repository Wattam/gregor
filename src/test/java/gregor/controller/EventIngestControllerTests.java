package gregor.controller;

import gregor.controller.advice.EventIngestControllerAdvice;
import gregor.dto.EventIngestRequest;
import gregor.service.WebhookService;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import tools.jackson.databind.ObjectMapper;

import java.util.UUID;

class EventIngestControllerTests {

    private static final UUID EVENT_ID = UUID.fromString("00000000-0000-0000-0000-000000000001");
    private final WebhookService webhookService = Mockito.mock(WebhookService.class);
    private final MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new EventIngestController(this.webhookService)).setControllerAdvice(new EventIngestControllerAdvice()).build();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @ParameterizedTest
    @ValueSource(strings = {"{}", "[1]", "\"value\"", "42", "true"})
    void acceptsEveryNonNullJsonPayloadType(String payload) throws Exception {
        EventIngestRequest eventIngestRequest = new EventIngestRequest("order.created", this.objectMapper.readTree(payload));
        Mockito.when(this.webhookService.ingestEvent(eventIngestRequest)).thenReturn(EVENT_ID);

        MockHttpServletResponse mockHttpServletResponse = this.post("{\"eventType\":\"order.created\",\"payload\":" + payload + "}");

        Assertions.assertThat(mockHttpServletResponse.getStatus()).isEqualTo(202);
        Assertions.assertThat(this.objectMapper.readTree(mockHttpServletResponse.getContentAsString()).get("eventId").asString()).isEqualTo("00000000-0000-0000-0000-000000000001");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"eventType\":",
            "{}",
            "{\"eventType\":\" \",\"payload\":{}}",
            "{\"eventType\":\"order.\\r\\ncreated\",\"payload\":{}}",
            "{\"eventType\":\"order.\\u0000created\",\"payload\":{}}",
            "{\"eventType\":\"order.created\"}",
            "{\"eventType\":\"order.created\",\"payload\":null}"
    })
    void rejectsInvalidRequestsBeforeIngesting(String body) throws Exception {
        MockHttpServletResponse mockHttpServletResponse = this.post(body);

        Assertions.assertThat(mockHttpServletResponse.getStatus()).isEqualTo(400);
        Assertions.assertThat(mockHttpServletResponse.getContentAsString()).contains("\"title\":\"Invalid request\"", "\"detail\":\"Request body is malformed or invalid.\"");
        Mockito.verifyNoInteractions(this.webhookService);
    }

    private MockHttpServletResponse post(String body) throws Exception {
        return this.mockMvc.perform(MockMvcRequestBuilders.post("/api/events").contentType(MediaType.APPLICATION_JSON).content(body)).andReturn().getResponse();
    }
}
