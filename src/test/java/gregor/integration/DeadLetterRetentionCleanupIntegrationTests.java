package gregor.integration;

import gregor.Application;
import gregor.avro.WebhookEnvelopeCodec;
import gregor.avro.WebhookEnvelopeFixtures;
import gregor.config.DeadLetterCleanupConfiguration;
import gregor.config.properties.DeadLetterCleanupProperties;
import gregor.dto.StoredWebhookEnvelope;
import gregor.entity.DeadLetterOccurrence;
import gregor.repository.DeadLetterOccurrenceCleanupRepository;
import gregor.repository.DeadLetterOccurrenceRepository;
import gregor.service.DeadLetterOccurrenceCleanupService;
import gregor.service.DeadLetterOccurrenceService;
import io.micrometer.core.instrument.MeterRegistry;
import org.assertj.core.api.Assertions;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.scheduling.concurrent.SimpleAsyncTaskScheduler;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DeadLetterRetentionCleanupIntegrationTests {

    private static final Instant KAFKA_TIMESTAMP = Instant.parse("2026-01-01T00:00:00Z");
    private static final Instant EXPIRY = Instant.parse("2026-01-31T00:00:00Z");
    private static final OffsetDateTime CUTOFF = OffsetDateTime.parse("2026-01-01T00:00:00Z");
    private static final Duration CLEANUP_INTERVAL = Duration.ofMillis(400);
    private static final String SCHEMA = "gregor_retention_it_" + UUID.randomUUID().toString().replace("-", "");
    private static final String DATABASE_URL = "jdbc:postgresql://%s:%s/%s".formatted(
            System.getenv().getOrDefault("POSTGRES_HOST", "localhost"),
            System.getenv().getOrDefault("POSTGRES_PORT", "5432"),
            System.getenv().getOrDefault("POSTGRES_DB", "gregor")
    );
    private static final String DATABASE_USER = System.getenv().getOrDefault("POSTGRES_USER", "gregor");
    private static final String DATABASE_PASSWORD = System.getenv().getOrDefault("POSTGRES_PASSWORD", "gregor");

    private final JdbcTemplate adminJdbcTemplate = new JdbcTemplate(new DriverManagerDataSource(DATABASE_URL, DATABASE_USER, DATABASE_PASSWORD));
    private final AtomicReference<Instant> now = new AtomicReference<>(EXPIRY);
    private final Clock clock = Mockito.mock(Clock.class);
    private ConfigurableApplicationContext configurableApplicationContext;
    private JdbcTemplate jdbcTemplate;
    private DeadLetterOccurrenceRepository deadLetterOccurrenceRepository;
    private DeadLetterOccurrenceCleanupService deadLetterOccurrenceCleanupService;
    private TransactionTemplate transactionTemplate;
    private MeterRegistry meterRegistry;

    @BeforeAll
    void startApplicationInAnIsolatedPostgresqlSchema() {
        Mockito.when(this.clock.instant()).thenAnswer(_ -> this.now.get());
        this.adminJdbcTemplate.execute("CREATE SCHEMA " + SCHEMA);
        // Start with the occurrence table from before the index upgrade.
        this.adminJdbcTemplate.execute(
                """
                CREATE TABLE %s.dead_letter_occurrence (
                    occurrence_id uuid PRIMARY KEY, event_id uuid NOT NULL, replay_count integer NOT NULL,
                    writer_schema text NOT NULL, datum_bytes bytea NOT NULL, dead_lettered_at timestamptz(3) NOT NULL,
                    replay_accepted boolean NOT NULL DEFAULT false,
                    CONSTRAINT uk_dead_letter_occurrence_event_replay UNIQUE (event_id, replay_count),
                    CONSTRAINT ck_dead_letter_occurrence_distinct_ids CHECK (occurrence_id <> event_id),
                    CONSTRAINT ck_dead_letter_occurrence_replay_count CHECK (replay_count >= 0)
                )
                """.formatted(SCHEMA)
        );
        this.configurableApplicationContext = this.startApplication("30d");
        this.jdbcTemplate = this.configurableApplicationContext.getBean(JdbcTemplate.class);
        this.deadLetterOccurrenceRepository = this.configurableApplicationContext.getBean(DeadLetterOccurrenceRepository.class);
        this.deadLetterOccurrenceCleanupService = this.configurableApplicationContext.getBean(DeadLetterOccurrenceCleanupService.class);
        this.transactionTemplate = new TransactionTemplate(this.configurableApplicationContext.getBean(PlatformTransactionManager.class));
        this.meterRegistry = this.configurableApplicationContext.getBean(MeterRegistry.class);
    }

    @AfterAll
    void closeApplicationAndDropOnlyItsTestSchema() {
        try {
            if (Objects.nonNull(this.configurableApplicationContext)) {
                this.configurableApplicationContext.close();
            }
        } finally {
            this.adminJdbcTemplate.execute("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE");
        }
    }

    @BeforeEach
    void resetOccurrenceFixtures() {
        this.jdbcTemplate.execute("TRUNCATE dead_letter_occurrence");
        this.now.set(EXPIRY);
        Mockito.when(this.clock.instant()).thenAnswer(_ -> this.now.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void filtersAllLookupsAtExactExpiryEvenInTheTransactionThatLoadedTheRow(boolean replayAccepted) {
        UUID occurrenceId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        this.insert(occurrenceId, eventId, 2, KAFKA_TIMESTAMP, replayAccepted);

        this.transactionTemplate.executeWithoutResult(_ -> {
            this.now.set(Instant.parse("2026-01-30T23:59:59.999Z"));
            assertAllLookupsRetain(this.deadLetterOccurrenceRepository, occurrenceId, eventId, 2);
            this.now.set(Instant.parse("2026-01-30T23:59:59.999999999Z"));
            assertAllLookupsRetain(this.deadLetterOccurrenceRepository, occurrenceId, eventId, 2);
            this.now.set(Instant.parse("2026-01-31T00:00:00.000Z"));
            assertAllLookupsEmpty(this.deadLetterOccurrenceRepository, occurrenceId, eventId, 2);
            this.now.set(Instant.parse("2026-01-31T00:00:00.001Z"));
            assertAllLookupsEmpty(this.deadLetterOccurrenceRepository, occurrenceId, eventId, 2);
            Assertions.assertThat(this.physicalIds()).containsExactly(occurrenceId);
        });
    }

    @Test
    void subtractsFractionalRetentionBeforeApplyingTheStoredTimestampPrecision() {
        UUID occurrenceId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        this.insert(occurrenceId, eventId, 0, KAFKA_TIMESTAMP, false);

        try (ConfigurableApplicationContext configuredApplicationContext = this.startApplication("PT720H0.000000500S")) {
            DeadLetterOccurrenceRepository deadLetterOccurrenceRepository = configuredApplicationContext.getBean(DeadLetterOccurrenceRepository.class);
            DeadLetterOccurrenceCleanupService deadLetterOccurrenceCleanupService = configuredApplicationContext.getBean(DeadLetterOccurrenceCleanupService.class);
            MeterRegistry meterRegistry = configuredApplicationContext.getBean(MeterRegistry.class);

            this.now.set(Instant.parse("2026-01-31T00:00:00.000000499Z"));
            assertAllLookupsRetain(deadLetterOccurrenceRepository, occurrenceId, eventId, 0);
            deadLetterOccurrenceCleanupService.runCleanup();
            Assertions.assertThat(this.physicalIds()).containsExactly(occurrenceId);
            Assertions.assertThat(meterRegistry.get("gregor.deadletter.occurrences.retained").gauge().value()).isEqualTo(1);
            Assertions.assertThat(meterRegistry.get("gregor.deadletter.cleanup.deleted").counter().count()).isZero();

            this.now.set(Instant.parse("2026-01-31T00:00:00.000000500Z"));
            assertAllLookupsEmpty(deadLetterOccurrenceRepository, occurrenceId, eventId, 0);
            Assertions.assertThat(this.physicalIds()).containsExactly(occurrenceId);
            deadLetterOccurrenceCleanupService.runCleanup();
            Assertions.assertThat(this.physicalIds()).isEmpty();
            Assertions.assertThat(meterRegistry.get("gregor.deadletter.occurrences.retained").gauge().value()).isZero();
            Assertions.assertThat(meterRegistry.get("gregor.deadletter.cleanup.deleted").counter().count()).isEqualTo(1);
        }
    }

    @Test
    void retainsRowsWhenTheCutoffPrecedesPostgresqlsTimestampRange() {
        UUID occurrenceId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        UUID oldestOccurrenceId = UUID.randomUUID();
        UUID oldestEventId = UUID.randomUUID();
        this.insert(occurrenceId, eventId, 0, KAFKA_TIMESTAMP, false);
        this.insert(oldestOccurrenceId, oldestEventId, 0, KAFKA_TIMESTAMP, true);
        this.jdbcTemplate.update("UPDATE dead_letter_occurrence SET dead_lettered_at = TIMESTAMPTZ '4714-11-24 00:00:00+00 BC' WHERE occurrence_id = ?", oldestOccurrenceId);

        try (ConfigurableApplicationContext configuredApplicationContext = this.startApplication("3000000d")) {
            DeadLetterOccurrenceRepository deadLetterOccurrenceRepository = configuredApplicationContext.getBean(DeadLetterOccurrenceRepository.class);
            DeadLetterOccurrenceCleanupService deadLetterOccurrenceCleanupService = configuredApplicationContext.getBean(DeadLetterOccurrenceCleanupService.class);
            MeterRegistry meterRegistry = configuredApplicationContext.getBean(MeterRegistry.class);

            assertAllLookupsRetain(deadLetterOccurrenceRepository, occurrenceId, eventId, 0);
            assertAllLookupsRetain(deadLetterOccurrenceRepository, oldestOccurrenceId, oldestEventId, 0);
            deadLetterOccurrenceCleanupService.runCleanup();
            Assertions.assertThat(this.physicalIds()).containsExactlyInAnyOrder(occurrenceId, oldestOccurrenceId);
            Assertions.assertThat(meterRegistry.get("gregor.deadletter.occurrences.retained").gauge().value()).isEqualTo(2);
            Assertions.assertThat(meterRegistry.get("gregor.deadletter.cleanup.runs").tag("outcome", "success").counter().count()).isEqualTo(1);

            Instant oldestExpiry = Instant.parse("-4713-11-24T00:00:00Z").plus(Duration.ofDays(3_000_000));
            this.now.set(oldestExpiry.minusNanos(1));
            assertAllLookupsRetain(deadLetterOccurrenceRepository, oldestOccurrenceId, oldestEventId, 0);

            this.now.set(oldestExpiry);
            assertAllLookupsEmpty(deadLetterOccurrenceRepository, oldestOccurrenceId, oldestEventId, 0);
            deadLetterOccurrenceCleanupService.runCleanup();
            Assertions.assertThat(this.physicalIds()).containsExactly(occurrenceId);
            Assertions.assertThat(meterRegistry.get("gregor.deadletter.occurrences.retained").gauge().value()).isEqualTo(1);
            Assertions.assertThat(meterRegistry.get("gregor.deadletter.cleanup.deleted").counter().count()).isEqualTo(1);
        }
    }

    @Test
    void latestFallsBackToTheHighestRetainedGenerationBeforeDeletion() {
        UUID eventId = UUID.randomUUID();
        UUID expiredOccurrenceId = UUID.randomUUID();
        UUID retainedOccurrenceId = UUID.randomUUID();
        UUID newerLowerGenerationId = UUID.randomUUID();
        this.insert(retainedOccurrenceId, eventId, 1, KAFKA_TIMESTAMP.plusMillis(1), true);
        this.insert(expiredOccurrenceId, eventId, 2, KAFKA_TIMESTAMP, false);
        this.insert(newerLowerGenerationId, eventId, 0, KAFKA_TIMESTAMP.plusSeconds(10), false);
        this.now.set(EXPIRY.minusMillis(1));
        assertAllLookupsRetain(this.deadLetterOccurrenceRepository, expiredOccurrenceId, eventId, 2);

        this.now.set(EXPIRY);
        assertAllLookupsRetain(this.deadLetterOccurrenceRepository, retainedOccurrenceId, eventId, 1);
        Assertions.assertThat(this.deadLetterOccurrenceRepository.findById(expiredOccurrenceId)).isEmpty();
        Assertions.assertThat(this.deadLetterOccurrenceRepository.findByEventIdAndReplayCount(eventId, 2)).isEmpty();
        Assertions.assertThat(this.physicalIds()).containsExactlyInAnyOrder(retainedOccurrenceId, expiredOccurrenceId, newerLowerGenerationId);
    }

    @Test
    void rejectsMissingOccurrenceIdForTheIdLookup() {
        Assertions.assertThatThrownBy(() -> this.deadLetterOccurrenceRepository.findById(null))
                .isInstanceOf(InvalidDataAccessApiUsageException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("The given id must not be null");
    }

    @Test
    void createsTheExpiryIndexOnAnExistingOccurrenceTable() {
        Assertions.assertThat(this.jdbcTemplate.queryForList(
                """
                SELECT attribute.attname::text FROM pg_index AS index_metadata
                JOIN pg_class AS index_relation ON index_relation.oid = index_metadata.indexrelid
                JOIN unnest(index_metadata.indkey) WITH ORDINALITY AS column_key(number, position) ON true
                JOIN pg_attribute AS attribute ON attribute.attrelid = index_metadata.indrelid AND attribute.attnum = column_key.number
                WHERE index_metadata.indrelid = 'dead_letter_occurrence'::regclass AND index_relation.relname = 'ix_dead_letter_occurrence_expiry'
                ORDER BY column_key.position
                """,
                String.class
        )).containsExactly("dead_lettered_at", "occurrence_id");
    }

    @Test
    void deletesOnlyTheOldestExpiredRowsInCommittedBatchesAndSamplesRetainedVolume() {
        UUID firstTieId = UUID.fromString("7fffffff-0000-0000-0000-000000000001");
        UUID secondTieId = UUID.fromString("80000000-0000-0000-0000-000000000001");
        UUID oldestId = UUID.randomUUID();
        UUID newerExpiredId = UUID.randomUUID();
        UUID expiryBoundaryId = UUID.randomUUID();
        UUID expiredEventId = UUID.randomUUID();
        UUID retainedId = UUID.randomUUID();
        UUID retainedEventId = UUID.randomUUID();
        this.insert(secondTieId, UUID.randomUUID(), 0, KAFKA_TIMESTAMP.minusMillis(2), false);
        this.insert(expiryBoundaryId, expiredEventId, 0, KAFKA_TIMESTAMP, true);
        this.insert(firstTieId, UUID.randomUUID(), 0, KAFKA_TIMESTAMP.minusMillis(2), true);
        this.insert(retainedId, retainedEventId, 0, KAFKA_TIMESTAMP.plusMillis(1), true);
        this.insert(newerExpiredId, UUID.randomUUID(), 0, KAFKA_TIMESTAMP.minusMillis(1), false);
        this.insert(oldestId, UUID.randomUUID(), 0, KAFKA_TIMESTAMP.minusMillis(3), false);
        StoredWebhookEnvelope retainedEnvelope = this.deadLetterOccurrenceRepository.findById(retainedId).orElseThrow().getStoredWebhookEnvelope();
        RunCounts previousRunCounts = this.runCounts();
        UUID attemptId = UUID.randomUUID();
        this.jdbcTemplate.update(
                """
                INSERT INTO delivery_attempt (
                    attempt_id, event_id, tenant_id, endpoint_id, event_type, attempt_number, outcome, latency_ms, attempted_at,
                    details, retry_count, replay_count, dead_lettered
                )
                VALUES (?, ?, ?, ?, 'cleanup-audit-fixture', 1, 'PERMANENT_FAILURE', 7, ?, '{}'::jsonb, 0, 0, true)
                """,
                attemptId,
                expiredEventId,
                UUID.randomUUID(),
                UUID.randomUUID(),
                CUTOFF
        );

        this.deadLetterOccurrenceCleanupService.runCleanup();
        Assertions.assertThat(this.physicalIds()).containsExactlyInAnyOrder(secondTieId, newerExpiredId, expiryBoundaryId, retainedId);
        this.assertRunDeltas(previousRunCounts, 2, 1, 0);
        this.assertRetainedVolume(1);

        this.deadLetterOccurrenceCleanupService.runCleanup();
        Assertions.assertThat(this.physicalIds()).containsExactlyInAnyOrder(expiryBoundaryId, retainedId);
        this.assertRunDeltas(previousRunCounts, 4, 2, 0);
        this.assertRetainedVolume(1);

        this.deadLetterOccurrenceCleanupService.runCleanup();
        Assertions.assertThat(this.physicalIds()).containsExactly(retainedId);
        this.assertRunDeltas(previousRunCounts, 5, 3, 0);

        this.deadLetterOccurrenceCleanupService.runCleanup();
        Assertions.assertThat(this.physicalIds()).containsExactly(retainedId);
        this.assertRunDeltas(previousRunCounts, 5, 4, 0);
        this.assertRetainedVolume(1);
        DeadLetterOccurrence retainedDeadLetterOccurrence = this.deadLetterOccurrenceRepository.findById(retainedId).orElseThrow();
        Assertions.assertThat(retainedDeadLetterOccurrence.getEventId()).isEqualTo(retainedEventId);
        Assertions.assertThat(retainedDeadLetterOccurrence.getReplayCount()).isZero();
        Assertions.assertThat(retainedDeadLetterOccurrence.getDeadLetteredAt()).isEqualTo(Instant.parse("2026-01-01T00:00:00.001Z"));
        Assertions.assertThat(retainedDeadLetterOccurrence.isReplayAccepted()).isTrue();
        Assertions.assertThat(retainedDeadLetterOccurrence.getStoredWebhookEnvelope()).isEqualTo(retainedEnvelope);
        Assertions.assertThat(this.jdbcTemplate.queryForList("SELECT event_type FROM delivery_attempt WHERE attempt_id = ? AND event_id = ?", String.class, attemptId, expiredEventId))
                .containsExactly("cleanup-audit-fixture");
    }

    @Test
    void cleanupQueriesRequireAnExplicitTransactionAndCommittedBatchesSurviveAnOuterRollback() {
        DeadLetterOccurrenceCleanupRepository deadLetterOccurrenceCleanupRepository = this.configurableApplicationContext.getBean(DeadLetterOccurrenceCleanupRepository.class);
        UUID occurrenceId = UUID.randomUUID();
        this.insert(occurrenceId, UUID.randomUUID(), 0, KAFKA_TIMESTAMP, false);
        String cutoff = "2026-01-01 00:00:00.000+00:00 AD";
        Assertions.assertThatExceptionOfType(IllegalTransactionStateException.class).isThrownBy(() -> deadLetterOccurrenceCleanupRepository.deleteExpired(cutoff, 2));
        Assertions.assertThatExceptionOfType(IllegalTransactionStateException.class).isThrownBy(() -> deadLetterOccurrenceCleanupRepository.countRetained(cutoff));
        Assertions.assertThat(this.physicalIds()).containsExactly(occurrenceId);
        RunCounts previousRunCounts = this.runCounts();

        this.transactionTemplate.executeWithoutResult(transactionStatus -> {
            this.deadLetterOccurrenceCleanupService.runCleanup();
            transactionStatus.setRollbackOnly();
        });

        Assertions.assertThat(this.physicalIds()).isEmpty();
        this.assertRunDeltas(previousRunCounts, 1, 1, 0);
        this.assertRetainedVolume(0);
    }

    @Test
    void overlappingBatchesSkipEachOthersRowsAndAnOrdinaryReadDoesNotWait() throws InterruptedException {
        UUID firstId = UUID.randomUUID();
        UUID secondId = UUID.randomUUID();
        UUID thirdId = UUID.randomUUID();
        UUID fourthId = UUID.randomUUID();
        this.insert(firstId, UUID.randomUUID(), 0, KAFKA_TIMESTAMP, false);
        this.insert(secondId, UUID.randomUUID(), 0, KAFKA_TIMESTAMP.plusMillis(1), false);
        this.insert(thirdId, UUID.randomUUID(), 0, KAFKA_TIMESTAMP.plusMillis(2), false);
        this.insert(fourthId, UUID.randomUUID(), 0, KAFKA_TIMESTAMP.plusMillis(3), false);
        RunCounts previousRunCounts = this.runCounts();
        this.now.set(EXPIRY.minusMillis(1));
        CountDownLatch readCutoffCaptured = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        CountDownLatch firstBatchReadyToCommit = new CountDownLatch(1);
        CountDownLatch releaseFirstCommit = new CountDownLatch(1);
        AtomicInteger clockReads = new AtomicInteger();
        Mockito.when(this.clock.instant()).thenAnswer(_ -> {
            Instant observationTime = this.now.get();
            switch (clockReads.incrementAndGet()) {
                case 1 -> {
                    readCutoffCaptured.countDown();
                    awaitRelease(releaseRead);
                }
                case 2 -> TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                    @Override
                    public void beforeCommit(boolean readOnly) {
                        firstBatchReadyToCommit.countDown();
                        awaitRelease(releaseFirstCommit);
                    }
                });
                default -> {
                }
            }
            return observationTime;
        });

        try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                Future<?> retainedRead = executorService.submit(() -> Assertions.assertThat(this.deadLetterOccurrenceRepository.findById(firstId)).get().extracting(DeadLetterOccurrence::getOccurrenceId).isEqualTo(firstId));
                Assertions.assertThat(readCutoffCaptured.await(5, TimeUnit.SECONDS)).isTrue();
                this.now.set(EXPIRY.plusMillis(3));
                Future<?> firstBatch = executorService.submit(this.deadLetterOccurrenceCleanupService::runCleanup);
                Assertions.assertThat(firstBatchReadyToCommit.await(5, TimeUnit.SECONDS)).isTrue();
                this.assertRunDeltas(previousRunCounts, 0, 0, 0);

                releaseRead.countDown();
                Assertions.assertThat(retainedRead).succeedsWithin(5, TimeUnit.SECONDS);
                Assertions.assertThat(executorService.submit(this.deadLetterOccurrenceCleanupService::runCleanup)).succeedsWithin(5, TimeUnit.SECONDS);
                Assertions.assertThat(this.physicalIds()).containsExactlyInAnyOrder(firstId, secondId);
                this.assertRunDeltas(previousRunCounts, 2, 1, 0);

                releaseFirstCommit.countDown();
                Assertions.assertThat(firstBatch).succeedsWithin(5, TimeUnit.SECONDS);
            } finally {
                releaseRead.countDown();
                releaseFirstCommit.countDown();
            }
        }
        Assertions.assertThat(this.physicalIds()).isEmpty();
        this.assertRunDeltas(previousRunCounts, 4, 2, 0);
        this.deadLetterOccurrenceCleanupService.runCleanup();
        this.assertRunDeltas(previousRunCounts, 4, 3, 0);
    }

    @Test
    void replayAdmissionChecksRetentionAfterLockAndCleanupSkipsAdmittedWorkUntilRelease() {
        UUID replayOccurrenceId = UUID.randomUUID();
        UUID replayEventId = UUID.randomUUID();
        UUID otherExpiredId = UUID.randomUUID();
        this.insert(replayOccurrenceId, replayEventId, 0, KAFKA_TIMESTAMP, false);
        this.insert(otherExpiredId, UUID.randomUUID(), 0, KAFKA_TIMESTAMP.plusMillis(1), true);
        this.now.set(EXPIRY.minusMillis(1));
        RunCounts previousRunCounts = this.runCounts();

        try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
            this.transactionTemplate.executeWithoutResult(_ -> {
                this.lockOccurrence(replayOccurrenceId);
                assertAllLookupsRetain(this.deadLetterOccurrenceRepository, replayOccurrenceId, replayEventId, 0);
                Assertions.assertThat(executorService.submit(() -> assertAllLookupsRetain(this.deadLetterOccurrenceRepository, replayOccurrenceId, replayEventId, 0))).succeedsWithin(5, TimeUnit.SECONDS);

                this.now.set(EXPIRY.plusMillis(1));
                assertAllLookupsEmpty(this.deadLetterOccurrenceRepository, replayOccurrenceId, replayEventId, 0);
                Assertions.assertThat(executorService.submit(this.deadLetterOccurrenceCleanupService::runCleanup)).succeedsWithin(5, TimeUnit.SECONDS);
                Assertions.assertThat(this.physicalIds()).containsExactly(replayOccurrenceId);
                this.assertRunDeltas(previousRunCounts, 1, 1, 0);
                this.assertRetainedVolume(0);
            });
        }

        this.transactionTemplate.executeWithoutResult(_ -> {
            this.lockOccurrence(replayOccurrenceId);
            assertAllLookupsEmpty(this.deadLetterOccurrenceRepository, replayOccurrenceId, replayEventId, 0);
        });
        this.deadLetterOccurrenceCleanupService.runCleanup();
        Assertions.assertThat(this.physicalIds()).isEmpty();
        this.assertRunDeltas(previousRunCounts, 2, 2, 0);
    }

    @Test
    void redeliveryRecreatesOnlyAnExpiredPhysicalRowAndANewerPublicationCanBeRetained() {
        UUID eventId = UUID.randomUUID();
        WebhookEnvelopeCodec webhookEnvelopeCodec = this.configurableApplicationContext.getBean(WebhookEnvelopeCodec.class);
        DeadLetterOccurrenceService deadLetterOccurrenceService = this.configurableApplicationContext.getBean(DeadLetterOccurrenceService.class);
        StoredWebhookEnvelope storedWebhookEnvelope = webhookEnvelopeCodec.encode(WebhookEnvelopeFixtures.currentEvent(eventId.toString(), 0, null));
        this.now.set(EXPIRY.minusMillis(1));
        deadLetterOccurrenceService.persistIfAbsent(storedWebhookEnvelope, KAFKA_TIMESTAMP);
        UUID originalOccurrenceId = this.physicalIds().getFirst();
        assertAllLookupsRetain(this.deadLetterOccurrenceRepository, originalOccurrenceId, eventId, 0);

        this.now.set(EXPIRY);
        this.deadLetterOccurrenceCleanupService.runCleanup();
        Assertions.assertThat(this.physicalIds()).isEmpty();
        deadLetterOccurrenceService.persistIfAbsent(storedWebhookEnvelope, KAFKA_TIMESTAMP);
        Assertions.assertThat(this.physicalIds()).hasSize(1).doesNotContain(originalOccurrenceId);
        UUID redeliveredOccurrenceId = this.physicalIds().getFirst();
        Assertions.assertThat(this.jdbcTemplate.queryForObject("SELECT dead_lettered_at FROM dead_letter_occurrence WHERE occurrence_id = ?", OffsetDateTime.class, redeliveredOccurrenceId)).isEqualTo(CUTOFF);
        assertAllLookupsEmpty(this.deadLetterOccurrenceRepository, redeliveredOccurrenceId, eventId, 0);
        this.deadLetterOccurrenceCleanupService.runCleanup();
        Assertions.assertThat(this.physicalIds()).isEmpty();

        deadLetterOccurrenceService.persistIfAbsent(storedWebhookEnvelope, Instant.parse("2026-01-01T00:00:00.001Z"));
        Assertions.assertThat(this.physicalIds()).hasSize(1).doesNotContain(originalOccurrenceId, redeliveredOccurrenceId);
        assertAllLookupsRetain(this.deadLetterOccurrenceRepository, this.physicalIds().getFirst(), eventId, 0);
    }

    @Test
    void scheduledCleanupWaitsAfterRegistrationAndSamplingAndRecoversAfterARejectedCommit() throws InterruptedException {
        UUID expiredId = UUID.randomUUID();
        UUID retainedId = UUID.randomUUID();
        this.insert(expiredId, UUID.randomUUID(), 0, KAFKA_TIMESTAMP, false);
        this.insert(retainedId, UUID.randomUUID(), 0, KAFKA_TIMESTAMP.plusMillis(1), false);
        RunCounts previousRunCounts = this.runCounts();
        this.installOneTimeCommitFailure();
        AtomicInteger clockReads = new AtomicInteger();
        CountDownLatch firstSampleStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstSample = new CountDownLatch(1);
        Mockito.when(this.clock.instant()).thenAnswer(_ -> {
            if (clockReads.incrementAndGet() == 2) {
                firstSampleStarted.countDown();
                if (!releaseFirstSample.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("First retained-volume sample was not released");
                }
            }
            return this.now.get();
        });

        try (AnnotationConfigApplicationContext annotationConfigApplicationContext = new AnnotationConfigApplicationContext()) {
            annotationConfigApplicationContext.register(DeadLetterCleanupConfiguration.class);
            annotationConfigApplicationContext.registerBean(DeadLetterOccurrenceCleanupService.class, () -> this.deadLetterOccurrenceCleanupService);
            annotationConfigApplicationContext.registerBean(DeadLetterCleanupProperties.class, () -> new DeadLetterCleanupProperties(CLEANUP_INTERVAL, "2"));
            annotationConfigApplicationContext.registerBean("taskScheduler", ObservingTaskScheduler.class);
            annotationConfigApplicationContext.refresh();
            ObservingTaskScheduler observingTaskScheduler = annotationConfigApplicationContext.getBean(ObservingTaskScheduler.class);
            try {
                long firstStart = awaitTimestamp(observingTaskScheduler.starts);
                Assertions.assertThat(firstStart - observingTaskScheduler.registrationNanos.get()).isGreaterThanOrEqualTo(CLEANUP_INTERVAL.toNanos());
                Assertions.assertThat(firstSampleStarted.await(5, TimeUnit.SECONDS)).isTrue();
                this.assertRunDeltas(previousRunCounts, 0, 0, 1);
                Assertions.assertThat(this.physicalIds()).containsExactlyInAnyOrder(expiredId, retainedId);
            } finally {
                releaseFirstSample.countDown();
            }

            long firstCompletion = awaitTimestamp(observingTaskScheduler.completions);
            long secondStart = awaitTimestamp(observingTaskScheduler.starts);
            Assertions.assertThat(secondStart - firstCompletion).isGreaterThanOrEqualTo(CLEANUP_INTERVAL.toNanos());
            awaitTimestamp(observingTaskScheduler.completions);
            try {
                this.assertRunDeltas(previousRunCounts, 1, 1, 1);
                this.assertRetainedVolume(1);
                Assertions.assertThat(this.physicalIds()).containsExactly(retainedId);
            } finally {
                observingTaskScheduler.releaseSecondCompletion.countDown();
            }
        } finally {
            this.jdbcTemplate.execute("DROP TRIGGER reject_first_cleanup ON dead_letter_occurrence");
            this.jdbcTemplate.execute("DROP FUNCTION reject_first_cleanup()");
            this.jdbcTemplate.execute("DROP SEQUENCE cleanup_commit_attempts");
        }
    }

    private ConfigurableApplicationContext startApplication(String retention) {
        return new SpringApplicationBuilder(Application.class)
                .web(WebApplicationType.NONE)
                .initializers(applicationContext -> ((GenericApplicationContext) applicationContext).registerBean("retentionTestClock", Clock.class, () -> this.clock, beanDefinition -> beanDefinition.setPrimary(true)))
                .run(
                        "--spring.config.location=file:./src/main/resources/application.yaml",
                        "--spring.datasource.url=" + DATABASE_URL + "?currentSchema=" + SCHEMA + "&options=-c%20statement_timeout%3D10000",
                        "--spring.datasource.username=" + DATABASE_USER,
                        "--spring.datasource.password=" + DATABASE_PASSWORD,
                        "--spring.kafka.admin.auto-create=false",
                        "--spring.kafka.bootstrap-servers=localhost:1",
                        "--camel.component.kafka.brokers=localhost:1",
                        "--camel.main.auto-startup=false",
                        "--gregor.dead-letter.cleanup-interval=1d",
                        "--gregor.dead-letter.cleanup-batch-size=2",
                        "--gregor.kafka.topics.dead-letter-retention=" + retention
                );
    }

    private void insert(UUID occurrenceId, UUID eventId, int replayCount, Instant deadLetteredAt, boolean replayAccepted) {
        this.jdbcTemplate.update(
                """
                INSERT INTO dead_letter_occurrence (occurrence_id, event_id, replay_count, dead_lettered_at, replay_accepted, writer_schema, datum_bytes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                occurrenceId,
                eventId,
                replayCount,
                deadLetteredAt.atOffset(ZoneOffset.UTC),
                replayAccepted,
                "{\"type\":\"null\",\"doc\":\"fixture schema\"}",
                new byte[]{8, -3, 5}
        );
    }

    private List<UUID> physicalIds() {
        return this.jdbcTemplate.queryForList("SELECT occurrence_id FROM dead_letter_occurrence", UUID.class);
    }

    private void lockOccurrence(UUID occurrenceId) {
        Assertions.assertThat(this.jdbcTemplate.queryForObject("SELECT occurrence_id FROM dead_letter_occurrence WHERE occurrence_id = ? FOR UPDATE", UUID.class, occurrenceId)).isEqualTo(occurrenceId);
    }

    private static void assertAllLookupsRetain(DeadLetterOccurrenceRepository deadLetterOccurrenceRepository, UUID occurrenceId, UUID eventId, int replayCount) {
        Assertions.assertThat(deadLetterOccurrenceRepository.findById(occurrenceId)).get().extracting(DeadLetterOccurrence::getOccurrenceId).isEqualTo(occurrenceId);
        Assertions.assertThat(deadLetterOccurrenceRepository.findByEventIdAndReplayCount(eventId, replayCount)).get().extracting(DeadLetterOccurrence::getOccurrenceId).isEqualTo(occurrenceId);
        Assertions.assertThat(deadLetterOccurrenceRepository.findFirstByEventIdOrderByReplayCountDesc(eventId)).get().extracting(DeadLetterOccurrence::getOccurrenceId).isEqualTo(occurrenceId);
    }

    private static void assertAllLookupsEmpty(DeadLetterOccurrenceRepository deadLetterOccurrenceRepository, UUID occurrenceId, UUID eventId, int replayCount) {
        Assertions.assertThat(deadLetterOccurrenceRepository.findById(occurrenceId)).isEmpty();
        Assertions.assertThat(deadLetterOccurrenceRepository.findByEventIdAndReplayCount(eventId, replayCount)).isEmpty();
        Assertions.assertThat(deadLetterOccurrenceRepository.findFirstByEventIdOrderByReplayCountDesc(eventId)).isEmpty();
    }

    private void installOneTimeCommitFailure() {
        // Sequence increments survive rollback, so PostgreSQL rejects the first commit and allows the next.
        this.jdbcTemplate.execute("CREATE SEQUENCE cleanup_commit_attempts");
        this.jdbcTemplate.execute(
                """
                CREATE FUNCTION reject_first_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    IF nextval('cleanup_commit_attempts') = 1 THEN
                        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'private cleanup fixture detail';
                    END IF;
                    RETURN OLD;
                END;
                $$
                """
        );
        this.jdbcTemplate.execute(
                """
                CREATE CONSTRAINT TRIGGER reject_first_cleanup AFTER DELETE ON dead_letter_occurrence
                DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_first_cleanup()
                """
        );
    }

    private RunCounts runCounts() {
        return new RunCounts(
                this.meterRegistry.get("gregor.deadletter.cleanup.deleted").counter().count(),
                this.meterRegistry.get("gregor.deadletter.cleanup.runs").tag("outcome", "success").counter().count(),
                this.meterRegistry.get("gregor.deadletter.cleanup.runs").tag("outcome", "failure").counter().count()
        );
    }

    private void assertRunDeltas(RunCounts previousRunCounts, int deleted, int successful, int failed) {
        RunCounts currentRunCounts = this.runCounts();
        Assertions.assertThat(currentRunCounts).isEqualTo(new RunCounts(previousRunCounts.deleted() + deleted, previousRunCounts.successful() + successful, previousRunCounts.failed() + failed));
    }

    private void assertRetainedVolume(int retained) {
        Assertions.assertThat(this.meterRegistry.get("gregor.deadletter.occurrences.retained").gauge().value()).isEqualTo(retained);
    }

    private static void awaitRelease(CountDownLatch countDownLatch) {
        try {
            if (!countDownLatch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Cleanup test latch was not released");
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Cleanup test latch was interrupted", exception);
        }
    }

    private static long awaitTimestamp(BlockingQueue<Long> timestamps) throws InterruptedException {
        Long timestamp = timestamps.poll(5, TimeUnit.SECONDS);
        Assertions.assertThat(timestamp).as("cleanup scheduler observation").isNotNull();
        return Objects.requireNonNull(timestamp);
    }

    private record RunCounts(double deleted, double successful, double failed) {
    }

    private static final class ObservingTaskScheduler extends SimpleAsyncTaskScheduler {

        private final AtomicLong registrationNanos = new AtomicLong();
        private final BlockingQueue<Long> starts = new LinkedBlockingQueue<>();
        private final BlockingQueue<Long> completions = new LinkedBlockingQueue<>();
        private final CountDownLatch releaseSecondCompletion = new CountDownLatch(1);
        private final AtomicInteger completedRuns = new AtomicInteger();

        ObservingTaskScheduler() {
            this.setVirtualThreads(true);
            long originNanos = System.nanoTime();
            InstantSource monotonicInstantSource = () -> Instant.EPOCH.plusNanos(System.nanoTime() - originNanos);
            this.setClock(monotonicInstantSource.withZone(ZoneOffset.UTC));
        }

        @Override
        @NullMarked
        public Clock getClock() {
            this.registrationNanos.compareAndSet(0L, System.nanoTime());
            return super.getClock();
        }

        @Override
        @NullMarked
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable runnable, Instant startTime, Duration delay) {
            return super.scheduleWithFixedDelay(() -> {
                this.starts.add(System.nanoTime());
                try {
                    runnable.run();
                } finally {
                    this.completions.add(System.nanoTime());
                    if (this.completedRuns.incrementAndGet() == 2) {
                        try {
                            this.releaseSecondCompletion.await(10, TimeUnit.SECONDS);
                        } catch (InterruptedException exception) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }, startTime, delay);
        }
    }
}
