package gregor.entity;

import gregor.dto.StoredWebhookEnvelope;
import jakarta.persistence.CheckConstraint;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityExistsException;
import jakarta.persistence.Id;
import jakarta.persistence.PersistenceException;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.ColumnDefault;
import org.hibernate.annotations.JavaType;
import org.hibernate.annotations.JdbcType;
import org.hibernate.annotations.SQLInsert;
import org.hibernate.dialect.type.PostgreSQLUUIDJdbcType;
import org.hibernate.jdbc.Expectation;
import org.hibernate.type.descriptor.ValueBinder;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.IntegerJavaType;
import org.hibernate.type.descriptor.java.PrimitiveByteArrayJavaType;
import org.hibernate.type.descriptor.java.StringJavaType;
import org.hibernate.type.descriptor.java.UUIDJavaType;
import org.hibernate.type.descriptor.jdbc.IntegerJdbcType;
import org.hibernate.type.descriptor.jdbc.VarbinaryJdbcType;
import org.hibernate.type.descriptor.jdbc.VarcharJdbcType;
import org.jspecify.annotations.NonNull;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.UUID;

@Getter
@Entity
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Table(
        name = "dead_letter_occurrence",
        uniqueConstraints = @UniqueConstraint(name = "uk_dead_letter_occurrence_event_replay", columnNames = {"event_id", "replay_count"}),
        check = {
                @CheckConstraint(name = "ck_dead_letter_occurrence_distinct_ids", constraint = "occurrence_id <> event_id"),
                @CheckConstraint(name = "ck_dead_letter_occurrence_replay_count", constraint = "replay_count >= 0")
        }
)
// Hibernate binds non-ID properties in name order, then the ID. Reject a zero-row insert without PostgreSQL's conflict details.
@SQLInsert(
        sql = """
              INSERT INTO dead_letter_occurrence (datum_bytes, dead_lettered_at, event_id, replay_accepted, replay_count, writer_schema, occurrence_id)
              VALUES (?, ?, ?, ?, ?, ?, ?)
              ON CONFLICT DO NOTHING
              """,
        verify = DeadLetterOccurrence.StrictInsert.class
)
public class DeadLetterOccurrence {

    @Id
    @Column(name = "occurrence_id")
    private UUID occurrenceId;

    @JavaType(RedactedEventIdJavaType.class)
    @JdbcType(EventIdJdbcType.class)
    @Column(name = "event_id", nullable = false, updatable = false)
    private UUID eventId;

    @JavaType(RedactedReplayCountJavaType.class)
    @JdbcType(ReplayCountJdbcType.class)
    @Column(name = "replay_count", nullable = false, updatable = false)
    private int replayCount;

    @Getter(AccessLevel.NONE)
    @JavaType(RedactedWriterSchemaJavaType.class)
    @JdbcType(WriterSchemaJdbcType.class)
    @Column(name = "writer_schema", nullable = false, updatable = false, columnDefinition = "TEXT")
    private String writerSchemaJson;

    @Getter(AccessLevel.NONE)
    @JavaType(RedactedDatumBytesJavaType.class)
    @JdbcType(DatumBytesJdbcType.class)
    @Column(name = "datum_bytes", nullable = false, updatable = false, columnDefinition = "BYTEA")
    private byte[] datumBytes;

    @Column(name = "dead_lettered_at", nullable = false, updatable = false, columnDefinition = "TIMESTAMPTZ(3)")
    private Instant deadLetteredAt;

    @ColumnDefault("false")
    @Column(name = "replay_accepted", nullable = false)
    private boolean replayAccepted;

    public DeadLetterOccurrence(UUID occurrenceId, UUID eventId, int replayCount, StoredWebhookEnvelope storedWebhookEnvelope, Instant deadLetteredAt) {
        this.occurrenceId = occurrenceId;
        this.eventId = eventId;
        this.replayCount = replayCount;
        this.writerSchemaJson = storedWebhookEnvelope.writerSchemaJson();
        this.datumBytes = storedWebhookEnvelope.datumBytes();
        this.deadLetteredAt = deadLetteredAt;
        this.replayAccepted = false;
    }

    public StoredWebhookEnvelope getStoredWebhookEnvelope() {
        return new StoredWebhookEnvelope(this.writerSchemaJson, this.datumBytes);
    }

    public static EntityExistsException duplicateConflict() {
        return new EntityExistsException("Dead-letter occurrence ID or generation already exists");
    }

    public static class StrictInsert implements Expectation {

        @Override
        public boolean canBeBatched() {
            return false;
        }

        @Override
        public void verifyOutcome(int rowCount, @NonNull PreparedStatement preparedStatement, int batchPosition, @NonNull String sql) {
            if (rowCount != 1) {
                throw duplicateConflict();
            }
        }
    }

    public static class RedactedEventIdJavaType extends UUIDJavaType {

        @Override
        public String extractLoggableRepresentation(UUID eventId) {
            return "[redacted]";
        }
    }

    public static class RedactedReplayCountJavaType extends IntegerJavaType {

        @Override
        public String extractLoggableRepresentation(Integer replayCount) {
            return "[redacted]";
        }
    }

    public static class RedactedWriterSchemaJavaType extends StringJavaType {

        @Override
        public String extractLoggableRepresentation(String writerSchemaJson) {
            return "[redacted]";
        }
    }

    public static class RedactedDatumBytesJavaType extends PrimitiveByteArrayJavaType {

        @Override
        public String extractLoggableRepresentation(byte[] datumBytes) {
            return "[redacted]";
        }
    }

    public static class EventIdJdbcType extends PostgreSQLUUIDJdbcType {

        @Override
        public <T> ValueBinder<T> getBinder(org.hibernate.type.descriptor.java.JavaType<T> javaType) {
            return new RedactingBinder<>(super.getBinder(javaType), "Dead-letter occurrence event ID could not be bound");
        }
    }

    public static class ReplayCountJdbcType extends IntegerJdbcType {

        @Override
        public <T> ValueBinder<T> getBinder(org.hibernate.type.descriptor.java.JavaType<T> javaType) {
            return new RedactingBinder<>(super.getBinder(javaType), "Dead-letter occurrence replay count could not be bound");
        }
    }

    public static class WriterSchemaJdbcType extends VarcharJdbcType {

        @Override
        public <T> ValueBinder<T> getBinder(org.hibernate.type.descriptor.java.JavaType<T> javaType) {
            return new RedactingBinder<>(super.getBinder(javaType), "Dead-letter occurrence writer schema could not be bound");
        }
    }

    public static class DatumBytesJdbcType extends VarbinaryJdbcType {

        @Override
        public <T> ValueBinder<T> getBinder(org.hibernate.type.descriptor.java.JavaType<T> javaType) {
            return new RedactingBinder<>(super.getBinder(javaType), "Dead-letter occurrence datum could not be bound");
        }
    }

    // Hibernate formats raw parameter values on SQLException. Discard the driver error before that path runs.
    private record RedactingBinder<T>(ValueBinder<T> valueBinder, String message) implements ValueBinder<T> {

        @Override
        public void bind(PreparedStatement preparedStatement, T value, int index, WrapperOptions wrapperOptions) {
            try {
                this.valueBinder.bind(preparedStatement, value, index, wrapperOptions);
            } catch (SQLException exception) {
                throw new PersistenceException(this.message);
            }
        }

        @Override
        public void bind(CallableStatement callableStatement, T value, String name, WrapperOptions wrapperOptions) {
            try {
                this.valueBinder.bind(callableStatement, value, name, wrapperOptions);
            } catch (SQLException exception) {
                throw new PersistenceException(this.message);
            }
        }

        @Override
        public Object getBindValue(T value, WrapperOptions wrapperOptions) {
            try {
                return this.valueBinder.getBindValue(value, wrapperOptions);
            } catch (SQLException exception) {
                throw new PersistenceException(this.message);
            }
        }
    }
}
