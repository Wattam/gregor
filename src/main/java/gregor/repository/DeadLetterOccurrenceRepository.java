package gregor.repository;

import gregor.entity.DeadLetterOccurrence;
import gregor.service.DeadLetterRetention;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.TypedQuery;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import java.util.Optional;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class DeadLetterOccurrenceRepository {

    private static final String RETAINED_JPQL = "SELECT occurrence FROM DeadLetterOccurrence occurrence WHERE occurrence.deadLetteredAt > cast(:cutoff as Instant) AND ";

    private final DeadLetterRetention deadLetterRetention;

    @PersistenceContext
    private EntityManager entityManager;

    /**
     * For replay-like work, first acquire the row lock in a write transaction with
     * {@code SELECT occurrence_id FROM dead_letter_occurrence WHERE occurrence_id = ? FOR UPDATE}.
     * Perform a fresh lookup in that transaction before starting work. An empty result rejects admission.
     * Hold the lock through the work and commit or rollback.
     */
    public Optional<DeadLetterOccurrence> findById(UUID occurrenceId) {
        Assert.notNull(occurrenceId, "The given id must not be null");
        TypedQuery<DeadLetterOccurrence> typedQuery = this.retainedQuery("occurrence.occurrenceId = :occurrenceId");
        typedQuery.setParameter("occurrenceId", occurrenceId);
        return Optional.ofNullable(typedQuery.getSingleResultOrNull());
    }

    public Optional<DeadLetterOccurrence> findByEventIdAndReplayCount(UUID eventId, int replayCount) {
        TypedQuery<DeadLetterOccurrence> typedQuery = this.retainedQuery("occurrence.eventId = :eventId AND occurrence.replayCount = :replayCount");
        typedQuery.setParameter("eventId", eventId);
        typedQuery.setParameter("replayCount", replayCount);
        return Optional.ofNullable(typedQuery.getSingleResultOrNull());
    }

    public Optional<DeadLetterOccurrence> findFirstByEventIdOrderByReplayCountDesc(UUID eventId) {
        TypedQuery<DeadLetterOccurrence> typedQuery = this.retainedQuery("occurrence.eventId = :eventId ORDER BY occurrence.replayCount DESC");
        typedQuery.setParameter("eventId", eventId);
        typedQuery.setMaxResults(1);
        return Optional.ofNullable(typedQuery.getSingleResultOrNull());
    }

    private TypedQuery<DeadLetterOccurrence> retainedQuery(String selection) {
        // JPQL preserves the mapped redacting bind types for event ID and replay count.
        TypedQuery<DeadLetterOccurrence> typedQuery = this.entityManager.createQuery(RETAINED_JPQL + selection, DeadLetterOccurrence.class);
        return typedQuery.setParameter("cutoff", this.deadLetterRetention.cutoff());
    }
}
