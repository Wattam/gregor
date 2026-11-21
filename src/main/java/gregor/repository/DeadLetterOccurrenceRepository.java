package gregor.repository;

import gregor.entity.DeadLetterOccurrence;
import org.springframework.data.repository.Repository;

import java.util.Optional;
import java.util.UUID;

public interface DeadLetterOccurrenceRepository extends Repository<DeadLetterOccurrence, UUID> {

    Optional<DeadLetterOccurrence> findById(UUID occurrenceId);

    Optional<DeadLetterOccurrence> findByEventIdAndReplayCount(UUID eventId, int replayCount);

    Optional<DeadLetterOccurrence> findFirstByEventIdOrderByReplayCountDesc(UUID eventId);
}
