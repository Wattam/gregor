package gregor.service;

import gregor.avro.ApicurioWebhookEnvelopeReader;
import gregor.avro.WebhookEnvelopeCodec;
import gregor.avro.WebhookEvent;
import gregor.dto.StoredWebhookEnvelope;
import gregor.entity.DeadLetterOccurrence;
import gregor.repository.DeadLetterOccurrenceWriter;
import jakarta.persistence.EntityExistsException;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.header.Headers;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class DeadLetterOccurrenceService {

    private final WebhookEnvelopeCodec webhookEnvelopeCodec;
    private final DeadLetterOccurrenceWriter deadLetterOccurrenceWriter;
    private final PlatformTransactionManager platformTransactionManager;
    private final ObjectProvider<ApicurioWebhookEnvelopeReader> objectProvider;

    @PersistenceContext
    private EntityManager entityManager;

    @Transactional
    public DeadLetterOccurrence insert(UUID occurrenceId, StoredWebhookEnvelope storedWebhookEnvelope, Instant deadLetteredAt) {
        if (Objects.isNull(occurrenceId)) {
            throw new IllegalArgumentException("Dead-letter occurrence ID is required");
        }
        Instant persistedDeadLetteredAt = normalizeDeadLetteredAt(deadLetteredAt);
        WebhookEvent webhookEvent = this.webhookEnvelopeCodec.decode(storedWebhookEnvelope);
        UUID eventId = parseEventId(webhookEvent.getEventId());
        if (Objects.equals(occurrenceId, eventId)) {
            throw new IllegalArgumentException("Dead-letter occurrence ID must differ from the event ID");
        }
        int replayCount = requireReplayCount(webhookEvent);
        DeadLetterOccurrence deadLetterOccurrence = new DeadLetterOccurrence(occurrenceId, eventId, replayCount, storedWebhookEnvelope, persistedDeadLetteredAt);
        try {
            this.entityManager.persist(deadLetterOccurrence);
        } catch (EntityExistsException exception) {
            // A duplicate ID already in the persistence context can fail before SQL runs.
            throw DeadLetterOccurrence.duplicateConflict();
        }
        this.entityManager.flush();
        return deadLetterOccurrence;
    }

    public Instant requireKafkaTimestamp(@Nullable Object timestamp) {
        if (!(timestamp instanceof Long timestampMillis) || timestampMillis < 0L) {
            throw new IllegalArgumentException("Dead-letter Kafka record timestamp must be nonnegative epoch milliseconds");
        }
        return Instant.ofEpochMilli(timestampMillis);
    }

    public void persistKafkaRecord(String topicName, Headers headers, byte[] valueBytes, Instant deadLetteredAt) {
        StoredWebhookEnvelope storedWebhookEnvelope = this.objectProvider.getObject().read(topicName, headers, valueBytes);
        this.persistIfAbsent(storedWebhookEnvelope, deadLetteredAt);
    }

    public void persistIfAbsent(StoredWebhookEnvelope storedWebhookEnvelope, Instant deadLetteredAt) {
        Instant persistedDeadLetteredAt = normalizeDeadLetteredAt(deadLetteredAt);
        WebhookEvent webhookEvent = this.webhookEnvelopeCodec.decode(storedWebhookEnvelope);
        UUID eventId = parseEventId(webhookEvent.getEventId());
        int replayCount = requireReplayCount(webhookEvent);
        UUID occurrenceId = createOccurrenceId(eventId);

        TransactionTemplate transactionTemplate = new TransactionTemplate(this.platformTransactionManager);
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        try {
            DeadLetterPersistenceDiagnostics.withoutRawDatabaseLogging(() -> transactionTemplate.executeWithoutResult(_ -> {
                int insertedRowCount = this.deadLetterOccurrenceWriter.insertIfAbsent(occurrenceId, eventId, replayCount, storedWebhookEnvelope, persistedDeadLetteredAt);
                if (insertedRowCount != 0 && insertedRowCount != 1) {
                    throw new IllegalStateException("Unexpected dead-letter occurrence insert count: " + insertedRowCount);
                }
            }));
        } catch (RuntimeException exception) {
            throw new IllegalStateException("Dead-letter occurrence transaction failed [sqlState=" + sanitizedSqlState(exception) + "]");
        }
    }

    private static UUID createOccurrenceId(UUID eventId) {
        UUID occurrenceId;
        do {
            occurrenceId = UUID.randomUUID();
        } while (Objects.equals(occurrenceId, eventId));
        return occurrenceId;
    }

    private static String sanitizedSqlState(RuntimeException exception) {
        if (NestedExceptionUtils.getMostSpecificCause(exception) instanceof SQLException sqlException) {
            String sqlState = sqlException.getSQLState();
            if (StringUtils.length(sqlState) == 5 && StringUtils.containsOnly(sqlState, "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")) {
                return sqlState;
            }
        }
        return "unknown";
    }

    private static Instant normalizeDeadLetteredAt(Instant deadLetteredAt) {
        if (Objects.isNull(deadLetteredAt)) {
            throw new IllegalArgumentException("Dead-letter timestamp is required");
        }
        return deadLetteredAt.truncatedTo(ChronoUnit.MILLIS);
    }

    private static int requireReplayCount(WebhookEvent webhookEvent) {
        int replayCount = webhookEvent.getReplayCount();
        if (replayCount < 0) {
            throw new IllegalArgumentException("Dead-letter replay count must be nonnegative");
        }
        return replayCount;
    }

    private static UUID parseEventId(String eventIdText) {
        try {
            UUID eventId = UUID.fromString(eventIdText);
            if (!Objects.equals(eventId.toString(), StringUtils.toRootLowerCase(eventIdText))) {
                throw new IllegalArgumentException();
            }
            return eventId;
        } catch (IllegalArgumentException | NullPointerException exception) {
            throw new IllegalArgumentException("Webhook envelope event ID must be a UUID");
        }
    }
}
