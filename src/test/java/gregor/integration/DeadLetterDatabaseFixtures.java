package gregor.integration;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class DeadLetterDatabaseFixtures {

    private static final int LOCK_NAMESPACE = 73215;
    private static final String PENDING_INSERT_SQL = """
                                                     SELECT EXISTS (
                                                         SELECT 1 FROM pg_locks AS gate
                                                         WHERE gate.locktype = 'advisory' AND NOT gate.granted AND gate.objsubid = 2
                                                           AND gate.classid::bigint = ? AND gate.objid::bigint = ?
                                                     """;

    static InsertGate holdInsert(JdbcTemplate jdbcTemplate, UUID eventId) throws SQLException {
        return holdInsert(jdbcTemplate, eventId, TriggerPhase.AFTER_INSERT, "");
    }

    static InsertGate holdInsert(JdbcTemplate jdbcTemplate, UUID eventId, TriggerPhase triggerPhase, String actionSql) throws SQLException {
        int lockId = ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
        String lockCall = "pg_advisory_xact_lock(%d, %d)".formatted(LOCK_NAMESPACE, lockId);
        Connection connection = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection();
        try {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT " + lockCall);
            }
            AutoCloseable insertTrigger = installTrigger(jdbcTemplate, eventId, triggerPhase, "PERFORM " + lockCall + ";\n" + actionSql);
            return new InsertGate(jdbcTemplate, lockId, connection, insertTrigger);
        } catch (SQLException | RuntimeException exception) {
            try {
                connection.close();
            } catch (SQLException closeException) {
                exception.addSuppressed(closeException);
            }
            throw exception;
        }
    }

    static AutoCloseable installTrigger(JdbcTemplate jdbcTemplate, UUID eventId, TriggerPhase triggerPhase, String actionSql) {
        String fixtureName = "gregor_it_" + UUID.randomUUID().toString().replace("-", "");
        jdbcTemplate.execute(
                """
                CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $fixture$
                BEGIN
                    %s
                    RETURN NEW;
                END;
                $fixture$
                """.formatted(fixtureName, actionSql)
        );
        String createTriggerSql = switch (triggerPhase) {
            case BEFORE_INSERT -> "CREATE TRIGGER %s BEFORE INSERT ON dead_letter_occurrence".formatted(fixtureName);
            case AFTER_INSERT -> "CREATE TRIGGER %s AFTER INSERT ON dead_letter_occurrence".formatted(fixtureName);
            case AT_COMMIT -> "CREATE CONSTRAINT TRIGGER %s AFTER INSERT ON dead_letter_occurrence DEFERRABLE INITIALLY DEFERRED".formatted(fixtureName);
        };
        try {
            jdbcTemplate.execute(createTriggerSql + " FOR EACH ROW WHEN (NEW.event_id = '%s'::uuid) EXECUTE FUNCTION %s()".formatted(eventId, fixtureName));
        } catch (RuntimeException exception) {
            try {
                jdbcTemplate.execute("DROP FUNCTION " + fixtureName + "()");
            } catch (RuntimeException cleanupException) {
                exception.addSuppressed(cleanupException);
            }
            throw exception;
        }
        return () -> {
            jdbcTemplate.execute("DROP TRIGGER " + fixtureName + " ON dead_letter_occurrence");
            jdbcTemplate.execute("DROP FUNCTION " + fixtureName + "()");
        };
    }

    enum TriggerPhase {
        BEFORE_INSERT, AFTER_INSERT, AT_COMMIT
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    static final class InsertGate implements AutoCloseable {

        private final JdbcTemplate jdbcTemplate;
        private final int lockId;
        private final Connection holdingConnection;
        private final AutoCloseable insertTrigger;

        void awaitPendingInsert() throws InterruptedException {
            this.awaitLock(PENDING_INSERT_SQL + ")", "insert held inside PostgreSQL");
        }

        void awaitCompetingInsert() throws InterruptedException {
            this.awaitLock(
                    PENDING_INSERT_SQL + """
                                           AND EXISTS (
                                               SELECT 1 FROM pg_locks AS duplicate
                                               WHERE duplicate.locktype = 'transactionid' AND NOT duplicate.granted
                                                 AND gate.pid = ANY(pg_blocking_pids(duplicate.pid))
                                           )
                                         )
                                         """,
                    "duplicate waiting for the first insert"
            );
        }

        private void awaitLock(String query, String description) throws InterruptedException {
            Instant deadline = Instant.now().plusSeconds(20);
            while (Instant.now().isBefore(deadline)) {
                if (Objects.equals(Boolean.TRUE, this.jdbcTemplate.queryForObject(query, Boolean.class, LOCK_NAMESPACE, this.lockId))) {
                    return;
                }
                Thread.sleep(50L);
            }
            throw new AssertionError("Did not observe " + description);
        }

        void release() throws SQLException {
            this.holdingConnection.rollback();
        }

        @Override
        public void close() throws Exception {
            try (AutoCloseable _ = this.insertTrigger; Connection _ = this.holdingConnection) {
                this.release();
            }
        }
    }
}
