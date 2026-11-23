package gregor.repository;

import gregor.dto.StoredWebhookEnvelope;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class DeadLetterOccurrenceWriter {

    private static final String INSERT_SQL = """
                                             INSERT INTO dead_letter_occurrence (
                                                 occurrence_id, event_id, replay_count, writer_schema, datum_bytes, dead_lettered_at, replay_accepted
                                             ) VALUES (?, ?, ?, ?, ?, ?, false)
                                             ON CONFLICT (event_id, replay_count) DO NOTHING
                                             """;

    private final JdbcTemplate jdbcTemplate;

    @Transactional(propagation = Propagation.MANDATORY)
    public int insertIfAbsent(UUID occurrenceId, UUID eventId, int replayCount, StoredWebhookEnvelope storedWebhookEnvelope, Instant deadLetteredAt) {
        ConnectionCallback<Integer> connectionCallback = connection -> {
            try (PreparedStatement preparedStatement = connection.prepareStatement(INSERT_SQL)) {
                preparedStatement.setObject(1, occurrenceId);
                preparedStatement.setObject(2, eventId);
                preparedStatement.setInt(3, replayCount);
                preparedStatement.setString(4, storedWebhookEnvelope.writerSchemaJson());
                preparedStatement.setBytes(5, storedWebhookEnvelope.datumBytes());
                preparedStatement.setObject(6, deadLetteredAt.atOffset(ZoneOffset.UTC));
                return preparedStatement.executeUpdate();
            } catch (SQLException exception) {
                throw new SQLException("Dead-letter occurrence insert failed", exception.getSQLState());
            }
        };
        return this.jdbcTemplate.execute(connectionCallback);
    }
}
