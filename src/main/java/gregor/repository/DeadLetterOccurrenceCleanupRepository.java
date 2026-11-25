package gregor.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;

@Repository
@RequiredArgsConstructor
public class DeadLetterOccurrenceCleanupRepository {

    private static final String DELETE_EXPIRED_SQL = """
                                                     WITH batch AS MATERIALIZED (
                                                         SELECT occurrence_id
                                                         FROM dead_letter_occurrence
                                                         WHERE dead_lettered_at <= CAST(? AS timestamptz)
                                                         ORDER BY dead_lettered_at, occurrence_id
                                                         LIMIT ?
                                                         FOR UPDATE SKIP LOCKED
                                                     )
                                                     DELETE FROM dead_letter_occurrence AS occurrence
                                                     USING batch
                                                     WHERE occurrence.occurrence_id = batch.occurrence_id
                                                     """;
    private static final String COUNT_RETAINED_SQL = "SELECT count(*) FROM dead_letter_occurrence WHERE dead_lettered_at > CAST(? AS timestamptz)";

    private final JdbcTemplate jdbcTemplate;

    @Transactional(propagation = Propagation.MANDATORY)
    public int deleteExpired(String cutoff, int batchSize) {
        return this.jdbcTemplate.update(DELETE_EXPIRED_SQL, cutoff, batchSize);
    }

    @Transactional(propagation = Propagation.MANDATORY, readOnly = true)
    public long countRetained(String cutoff) {
        return Objects.requireNonNull(this.jdbcTemplate.queryForObject(COUNT_RETAINED_SQL, Long.class, cutoff));
    }
}
