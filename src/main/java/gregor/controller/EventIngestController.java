package gregor.controller;

import gregor.dto.EventIngestRequest;
import gregor.dto.EventIngestResponse;
import gregor.service.WebhookService;
import jakarta.validation.Valid;
import lombok.AllArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@AllArgsConstructor
@RequestMapping("/api/events")
public class EventIngestController {

    private final WebhookService webhookService;

    @PostMapping
    public ResponseEntity<EventIngestResponse> ingest(@Valid @RequestBody EventIngestRequest eventIngestRequest) {
        UUID eventId = this.webhookService.ingestEvent(eventIngestRequest);
        return ResponseEntity.accepted().body(new EventIngestResponse(eventId));
    }
}
