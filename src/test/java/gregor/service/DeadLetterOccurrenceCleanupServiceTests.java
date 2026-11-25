package gregor.service;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import gregor.config.properties.DeadLetterCleanupProperties;
import gregor.config.properties.KafkaTopicProperties;
import gregor.repository.DeadLetterOccurrenceCleanupRepository;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

class DeadLetterOccurrenceCleanupServiceTests {

    private static final Instant NOW = Instant.parse("2026-01-31T00:00:00Z");
    private static final String CUTOFF = "2026-01-01 00:00:00.000+00:00 AD";
    private static final String RETAINED = "gregor.deadletter.occurrences.retained";
    private static final String DELETED = "gregor.deadletter.cleanup.deleted";
    private static final String RUNS = "gregor.deadletter.cleanup.runs";

    private final Connection connection = Mockito.mock(Connection.class);
    private final DataSource dataSource = Mockito.mock(DataSource.class);
    private final DeadLetterOccurrenceCleanupRepository deadLetterOccurrenceCleanupRepository = Mockito.mock(DeadLetterOccurrenceCleanupRepository.class);
    private final Clock clock = Mockito.mock(Clock.class);
    private final SimpleMeterRegistry simpleMeterRegistry = new SimpleMeterRegistry();
    private JdbcTransactionManager jdbcTransactionManager;

    @BeforeEach
    void setUpTransactionManager() throws SQLException {
        Mockito.when(this.dataSource.getConnection()).thenReturn(this.connection);
        Mockito.when(this.connection.getAutoCommit()).thenReturn(true);
        Mockito.when(this.clock.instant()).thenReturn(NOW);
        this.jdbcTransactionManager = new JdbcTransactionManager(this.dataSource);
    }

    @AfterEach
    void closeRegistry() {
        this.simpleMeterRegistry.close();
    }

    @Test
    void registersOnlyBoundedMetersAndSamplesOnASuccessfulEmptyBatchWithAFreshCutoff() {
        DeadLetterOccurrenceCleanupService deadLetterOccurrenceCleanupService = this.cleanupService(this.jdbcTransactionManager);
        Mockito.when(this.clock.instant()).thenReturn(NOW, NOW.plusMillis(1));
        Mockito.when(this.deadLetterOccurrenceCleanupRepository.deleteExpired(CUTOFF, 2)).thenAnswer(_ -> {
            Assertions.assertThat(TransactionSynchronizationManager.isCurrentTransactionReadOnly()).isFalse();
            Assertions.assertThat(TransactionSynchronizationManager.getCurrentTransactionIsolationLevel()).isEqualTo(Connection.TRANSACTION_READ_COMMITTED);
            return 0;
        });
        Mockito.when(this.deadLetterOccurrenceCleanupRepository.countRetained("2026-01-01 00:00:00.001+00:00 AD")).thenAnswer(_ -> {
            Assertions.assertThat(TransactionSynchronizationManager.isCurrentTransactionReadOnly()).isTrue();
            Assertions.assertThat(TransactionSynchronizationManager.getCurrentTransactionIsolationLevel()).isEqualTo(Connection.TRANSACTION_READ_COMMITTED);
            return 7L;
        });

        this.assertMetrics(Double.NaN, 0, 0, 0);
        Assertions.assertThat(this.simpleMeterRegistry.getMeters())
                .extracting(Meter::getId)
                .extracting(meterId -> Assertions.tuple(meterId.getName(), meterId.getType(), meterId.getTags()))
                .containsExactlyInAnyOrder(
                        Assertions.tuple(RETAINED, Meter.Type.GAUGE, List.of()),
                        Assertions.tuple(DELETED, Meter.Type.COUNTER, List.of()),
                        Assertions.tuple(RUNS, Meter.Type.COUNTER, List.of(Tag.of("outcome", "success"))),
                        Assertions.tuple(RUNS, Meter.Type.COUNTER, List.of(Tag.of("outcome", "failure")))
                );

        deadLetterOccurrenceCleanupService.runCleanup();

        this.assertMetrics(7, 0, 1, 0);
    }

    @Test
    void retainsThePreviousSampleWhenCountingFailsAndUpdatesItAfterRecovery() {
        DeadLetterOccurrenceCleanupService deadLetterOccurrenceCleanupService = this.cleanupService(this.jdbcTransactionManager);
        Mockito.when(this.deadLetterOccurrenceCleanupRepository.deleteExpired(CUTOFF, 2)).thenReturn(0, 2, 0);
        Mockito.when(this.deadLetterOccurrenceCleanupRepository.countRetained(CUTOFF)).thenReturn(7L).thenThrow(new IllegalStateException("private count detail")).thenReturn(4L);

        deadLetterOccurrenceCleanupService.runCleanup();
        this.assertMetrics(7, 0, 1, 0);
        deadLetterOccurrenceCleanupService.runCleanup();
        this.assertMetrics(7, 2, 2, 0);
        deadLetterOccurrenceCleanupService.runCleanup();
        this.assertMetrics(4, 2, 3, 0);
    }

    @Test
    void doesNotPublishADeletionOrSampleFromTransactionsThatReturnNormallyAfterRollback() {
        PlatformTransactionManager platformTransactionManager = Mockito.mock(PlatformTransactionManager.class, AdditionalAnswers.delegatesTo(this.jdbcTransactionManager));
        Mockito.doAnswer(invocationOnMock -> {
            TransactionStatus transactionStatus = this.jdbcTransactionManager.getTransaction(invocationOnMock.getArgument(0, TransactionDefinition.class));
            transactionStatus.setRollbackOnly();
            return transactionStatus;
        }).when(platformTransactionManager).getTransaction(Mockito.any());
        DeadLetterOccurrenceCleanupService deadLetterOccurrenceCleanupService = this.cleanupService(platformTransactionManager);
        Mockito.when(this.deadLetterOccurrenceCleanupRepository.deleteExpired(CUTOFF, 2)).thenReturn(2);
        Mockito.when(this.deadLetterOccurrenceCleanupRepository.countRetained(CUTOFF)).thenReturn(7L);

        deadLetterOccurrenceCleanupService.runCleanup();

        this.assertMetrics(Double.NaN, 0, 0, 1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void doesNotCountADeletionIfCommitThrowsEvenWhenCompletionWasConfirmed(boolean committedBeforeError) throws SQLException {
        PlatformTransactionManager platformTransactionManager = Mockito.mock(PlatformTransactionManager.class, AdditionalAnswers.delegatesTo(this.jdbcTransactionManager));
        if (committedBeforeError) {
            AtomicBoolean firstCommit = new AtomicBoolean(true);
            Mockito.doAnswer(invocationOnMock -> {
                this.jdbcTransactionManager.commit(invocationOnMock.getArgument(0, TransactionStatus.class));
                if (firstCommit.getAndSet(false)) {
                    throw new IllegalStateException("private completion failure");
                }
                return null;
            }).when(platformTransactionManager).commit(Mockito.any());
        } else {
            Mockito.doThrow(new SQLException("private unknown commit outcome", "08006")).doNothing().when(this.connection).commit();
        }
        DeadLetterOccurrenceCleanupService deadLetterOccurrenceCleanupService = this.cleanupService(platformTransactionManager);
        Mockito.when(this.deadLetterOccurrenceCleanupRepository.deleteExpired(CUTOFF, 2)).thenReturn(2, committedBeforeError ? 0 : 2);
        Mockito.when(this.deadLetterOccurrenceCleanupRepository.countRetained(CUTOFF)).thenReturn(7L);

        deadLetterOccurrenceCleanupService.runCleanup();
        this.assertMetrics(7, 0, 0, 1);
        deadLetterOccurrenceCleanupService.runCleanup();
        this.assertMetrics(7, committedBeforeError ? 0 : 2, 1, 1);
    }

    @Test
    void logsAggregateOutcomesWithoutPrivateDatabaseDetails() throws SQLException {
        DeadLetterOccurrenceCleanupService deadLetterOccurrenceCleanupService = this.cleanupService(this.jdbcTransactionManager);
        Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        Logger databaseLogger = (Logger) LoggerFactory.getLogger("org.springframework.jdbc.core.CleanupFixture");
        Logger cleanupLogger = (Logger) LoggerFactory.getLogger(DeadLetterOccurrenceCleanupService.class);
        Level previousDatabaseLevel = databaseLogger.getLevel();
        Level previousCleanupLevel = cleanupLogger.getLevel();
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        rootLogger.addAppender(listAppender);
        databaseLogger.setLevel(Level.ERROR);
        cleanupLogger.setLevel(Level.INFO);
        String privateDetail = "private-driver-detail 12345678-0000-0000-0000-123456789abc private-payload private-schema whsec_private-signing";
        SQLException sqlException = new SQLException(privateDetail, "23514");
        try {
            Mockito.when(this.dataSource.getConnection()).thenAnswer(_ -> {
                databaseLogger.error("Private transaction entry: {}", privateDetail, sqlException);
                return this.connection;
            });
            Mockito.doAnswer(_ -> {
                databaseLogger.error("Private commit: {}", privateDetail, sqlException);
                return null;
            }).when(this.connection).commit();
            Mockito.doAnswer(_ -> {
                databaseLogger.error("Private rollback: {}", privateDetail, sqlException);
                return null;
            }).when(this.connection).rollback();
            Mockito.when(this.deadLetterOccurrenceCleanupRepository.deleteExpired(CUTOFF, 2)).thenAnswer(_ -> {
                databaseLogger.error("Private deletion: {}", privateDetail, sqlException);
                throw new IllegalStateException(privateDetail, sqlException);
            }).thenReturn(2);
            Mockito.when(this.deadLetterOccurrenceCleanupRepository.countRetained(CUTOFF)).thenAnswer(_ -> {
                databaseLogger.error("Private count: {}", privateDetail, sqlException);
                throw new IllegalStateException(privateDetail, sqlException);
            }).thenReturn(7L);

            deadLetterOccurrenceCleanupService.runCleanup();
            this.assertMetrics(Double.NaN, 0, 0, 1);
            deadLetterOccurrenceCleanupService.runCleanup();
            this.assertMetrics(7, 2, 1, 1);

            Assertions.assertThat(listAppender.list).allSatisfy(loggingEvent -> Assertions.assertThat(loggingEvent.getFormattedMessage()).doesNotContain(
                    "private-driver-detail",
                    "12345678-0000-0000-0000-123456789abc",
                    "private-payload",
                    "private-schema",
                    "whsec_private-signing"
            ));
            List<ILoggingEvent> cleanupEvents = listAppender.list.stream().filter(loggingEvent -> Objects.equals(loggingEvent.getLoggerName(), DeadLetterOccurrenceCleanupService.class.getName())).toList();
            Assertions.assertThat(cleanupEvents)
                    .extracting(loggingEvent -> Assertions.tuple(loggingEvent.getLevel(), loggingEvent.getFormattedMessage(), loggingEvent.getThrowableProxy()))
                    .containsExactly(
                            Assertions.tuple(Level.WARN, "Dead-letter cleanup failed [outcome=failure]", null),
                            Assertions.tuple(Level.WARN, "Dead-letter retained count failed [sample=unchanged]", null),
                            Assertions.tuple(Level.INFO, "Dead-letter cleanup completed [outcome=success, deleted=2]", null),
                            Assertions.tuple(Level.INFO, "Sampled retained dead-letter occurrences [retained=7]", null)
                    );
        } finally {
            cleanupLogger.setLevel(previousCleanupLevel);
            databaseLogger.setLevel(previousDatabaseLevel);
            rootLogger.detachAppender(listAppender);
            listAppender.stop();
        }
    }

    private DeadLetterOccurrenceCleanupService cleanupService(PlatformTransactionManager platformTransactionManager) {
        return new DeadLetterOccurrenceCleanupService(
                this.deadLetterOccurrenceCleanupRepository,
                new DeadLetterCleanupProperties(Duration.ofMinutes(1), "2"),
                new DeadLetterRetention(this.clock, new KafkaTopicProperties(12, 1, Duration.ofDays(7), Duration.ofDays(30))),
                new DeadLetterCleanupMetrics(this.simpleMeterRegistry),
                platformTransactionManager
        );
    }

    private void assertMetrics(double retained, double deleted, double successes, double failures) {
        double currentRetained = this.simpleMeterRegistry.get(RETAINED).gauge().value();
        if (Double.isNaN(retained)) {
            Assertions.assertThat(currentRetained).isNaN();
        } else {
            Assertions.assertThat(currentRetained).isEqualTo(retained);
        }
        Assertions.assertThat(this.simpleMeterRegistry.get(DELETED).counter().count()).isEqualTo(deleted);
        Assertions.assertThat(this.simpleMeterRegistry.get(RUNS).tag("outcome", "success").counter().count()).isEqualTo(successes);
        Assertions.assertThat(this.simpleMeterRegistry.get(RUNS).tag("outcome", "failure").counter().count()).isEqualTo(failures);
    }
}
