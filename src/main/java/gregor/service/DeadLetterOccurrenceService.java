package gregor.service;

import gregor.avro.WebhookEnvelopeCodec;
import gregor.avro.WebhookEvent;
import gregor.dto.StoredWebhookEnvelope;
import gregor.entity.DeadLetterOccurrence;
import jakarta.persistence.EntityExistsException;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class DeadLetterOccurrenceService {

    private final WebhookEnvelopeCodec webhookEnvelopeCodec;

    @PersistenceContext
    private EntityManager entityManager;

    @Transactional
    public DeadLetterOccurrence insert(UUID occurrenceId, StoredWebhookEnvelope storedWebhookEnvelope, Instant deadLetteredAt) {
        if (occurrenceId == null) {
            throw new IllegalArgumentException("Dead-letter occurrence ID is required");
        }
        if (deadLetteredAt == null) {
            throw new IllegalArgumentException("Dead-letter timestamp is required");
        }
        WebhookEvent webhookEvent = this.webhookEnvelopeCodec.decode(storedWebhookEnvelope);
        UUID eventId = parseEventId(webhookEvent.getEventId());
        if (occurrenceId.equals(eventId)) {
            throw new IllegalArgumentException("Dead-letter occurrence ID must differ from the event ID");
        }
        int replayCount = webhookEvent.getReplayCount();
        if (replayCount < 0) {
            throw new IllegalArgumentException("Dead-letter replay count must be nonnegative");
        }
        DeadLetterOccurrence deadLetterOccurrence = new DeadLetterOccurrence(occurrenceId, eventId, replayCount, storedWebhookEnvelope, deadLetteredAt.truncatedTo(ChronoUnit.MILLIS));
        try {
            this.entityManager.persist(deadLetterOccurrence);
        } catch (EntityExistsException exception) {
            // A duplicate ID already in the persistence context can fail before SQL runs.
            throw DeadLetterOccurrence.duplicateConflict();
        }
        this.entityManager.flush();
        return deadLetterOccurrence;
    }

    private static UUID parseEventId(String eventIdText) {
        try {
            UUID eventId = UUID.fromString(eventIdText);
            if (!eventId.toString().equalsIgnoreCase(eventIdText)) {
                throw new IllegalArgumentException();
            }
            return eventId;
        } catch (IllegalArgumentException | NullPointerException exception) {
            throw new IllegalArgumentException("Webhook envelope event ID must be a UUID");
        }
    }
}
