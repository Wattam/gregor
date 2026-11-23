package gregor.service;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import gregor.avro.AvroClassSecurityInitializer;
import gregor.avro.WebhookEnvelopeCodec;
import gregor.avro.WebhookEnvelopeFixtures;
import gregor.dto.StoredWebhookEnvelope;
import gregor.exception.InvalidWebhookEnvelopeException;
import gregor.repository.DeadLetterOccurrenceWriter;
import org.apache.avro.util.ClassSecurityValidator;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

class DeadLetterOccurrenceServiceTests {

    private static final UUID EVENT_ID = UUID.fromString("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa");
    private static final UUID OCCURRENCE_ID = UUID.fromString("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb");
    private static final Instant DEAD_LETTERED_AT = Instant.parse("2026-11-22T10:03:00.123Z");
    private static ClassSecurityValidator.ClassSecurityPredicate previousClassSecurityPredicate;

    private final DeadLetterOccurrenceWriter deadLetterOccurrenceWriter = Mockito.mock(DeadLetterOccurrenceWriter.class);
    private final PlatformTransactionManager platformTransactionManager = Mockito.mock(PlatformTransactionManager.class);
    private final DeadLetterOccurrenceService deadLetterOccurrenceService = new DeadLetterOccurrenceService(new WebhookEnvelopeCodec(), this.deadLetterOccurrenceWriter, this.platformTransactionManager, Mockito.mock());
    private final SimpleTransactionStatus transactionStatus = new SimpleTransactionStatus();

    @BeforeAll
    static void configureAvroClassSecurity() {
        previousClassSecurityPredicate = ClassSecurityValidator.getGlobal();
        try (GenericApplicationContext genericApplicationContext = new GenericApplicationContext()) {
            new AvroClassSecurityInitializer().initialize(genericApplicationContext);
        }
    }

    @AfterAll
    static void restoreAvroClassSecurity() {
        ClassSecurityValidator.setGlobal(previousClassSecurityPredicate);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void commitsBothGenerationConflictsAndInsertsWithTheOriginalWriterData(int insertedRowCount) throws IOException {
        StoredWebhookEnvelope storedWebhookEnvelope = olderEnvelope();
        Mockito.when(this.platformTransactionManager.getTransaction(Mockito.any())).thenReturn(this.transactionStatus);
        Mockito.when(this.deadLetterOccurrenceWriter.insertIfAbsent(OCCURRENCE_ID, EVENT_ID, 0, storedWebhookEnvelope, DEAD_LETTERED_AT)).thenReturn(insertedRowCount);
        try (MockedStatic<UUID> mockedUuid = Mockito.mockStatic(UUID.class, Mockito.CALLS_REAL_METHODS)) {
            mockedUuid.when(UUID::randomUUID).thenReturn(EVENT_ID, OCCURRENCE_ID);
            this.deadLetterOccurrenceService.persistIfAbsent(storedWebhookEnvelope, Instant.parse("2026-11-22T10:03:00.123789Z"));
        }

        InOrder inOrder = Mockito.inOrder(this.platformTransactionManager, this.deadLetterOccurrenceWriter);
        inOrder.verify(this.platformTransactionManager).getTransaction(Mockito.argThat(
                transactionDefinition -> transactionDefinition.getPropagationBehavior() == TransactionDefinition.PROPAGATION_REQUIRES_NEW
                        && transactionDefinition.getIsolationLevel() == TransactionDefinition.ISOLATION_DEFAULT
        ));
        inOrder.verify(this.deadLetterOccurrenceWriter).insertIfAbsent(Mockito.eq(OCCURRENCE_ID), Mockito.eq(EVENT_ID), Mockito.eq(0), Mockito.same(storedWebhookEnvelope), Mockito.eq(DEAD_LETTERED_AT));
        inOrder.verify(this.platformTransactionManager).commit(this.transactionStatus);
        inOrder.verifyNoMoreInteractions();
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 2})
    void rollsBackAnUnexpectedWriteCountWithoutReportingSuccess(int insertedRowCount) throws IOException {
        StoredWebhookEnvelope storedWebhookEnvelope = olderEnvelope();
        Mockito.when(this.platformTransactionManager.getTransaction(Mockito.any())).thenReturn(this.transactionStatus);
        Mockito.when(this.deadLetterOccurrenceWriter.insertIfAbsent(Mockito.any(UUID.class), Mockito.eq(EVENT_ID), Mockito.eq(0), Mockito.same(storedWebhookEnvelope), Mockito.eq(DEAD_LETTERED_AT))).thenReturn(insertedRowCount);

        assertTransactionFailure(() -> this.deadLetterOccurrenceService.persistIfAbsent(storedWebhookEnvelope, DEAD_LETTERED_AT), "unknown");

        Mockito.verify(this.platformTransactionManager).rollback(this.transactionStatus);
        Mockito.verify(this.platformTransactionManager, Mockito.never()).commit(Mockito.any());
    }

    @ParameterizedTest
    @CsvSource({
            "BEGIN, 08006, 08006",
            "COMMIT_INSERTED, 0A000, 0A000",
            "COMMIT_DUPLICATE, 08006, 08006",
            "WRITE, 23505, 23505",
            "WRITE, , unknown",
            "WRITE, 2350, unknown",
            "WRITE, 235050, unknown",
            "WRITE, 0a000, unknown",
            "WRITE, 23_14, unknown"
    })
    void exposesOnlyAnAllowlistedSqlStateWhenTheTransactionFails(FailurePhase failurePhase, String rawSqlState, String expectedSqlState) throws IOException {
        StoredWebhookEnvelope storedWebhookEnvelope = olderEnvelope();
        SQLException sqlException = new SQLException("private driver and envelope data", rawSqlState);
        sqlException.addSuppressed(new SQLException("private cleanup data"));
        IllegalStateException rawException = new IllegalStateException("private transaction detail", sqlException);
        rawException.addSuppressed(new IllegalStateException("private transaction cleanup"));
        Mockito.when(this.platformTransactionManager.getTransaction(Mockito.any())).thenReturn(this.transactionStatus);
        switch (failurePhase) {
            case BEGIN -> Mockito.when(this.platformTransactionManager.getTransaction(Mockito.any())).thenThrow(rawException);
            case WRITE -> Mockito.when(this.deadLetterOccurrenceWriter.insertIfAbsent(Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.any(), Mockito.any())).thenThrow(rawException);
            case COMMIT_INSERTED, COMMIT_DUPLICATE -> {
                int insertedRowCount = Objects.equals(failurePhase, FailurePhase.COMMIT_INSERTED) ? 1 : 0;
                Mockito.when(this.deadLetterOccurrenceWriter.insertIfAbsent(Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.any(), Mockito.any())).thenReturn(insertedRowCount);
                Mockito.doThrow(rawException).when(this.platformTransactionManager).commit(this.transactionStatus);
            }
        }

        assertTransactionFailure(() -> this.deadLetterOccurrenceService.persistIfAbsent(storedWebhookEnvelope, DEAD_LETTERED_AT), expectedSqlState);

        if (Objects.equals(failurePhase, FailurePhase.BEGIN)) {
            Mockito.verifyNoInteractions(this.deadLetterOccurrenceWriter);
        }
        if (Objects.equals(failurePhase, FailurePhase.WRITE)) {
            Mockito.verify(this.platformTransactionManager).rollback(this.transactionStatus);
            Mockito.verify(this.platformTransactionManager, Mockito.never()).commit(Mockito.any());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void suppressesDatabaseDiagnosticsThroughTransactionCompletion(boolean failsAtWrite) throws IOException {
        StoredWebhookEnvelope storedWebhookEnvelope = olderEnvelope();
        Logger databaseLogger = (Logger) LoggerFactory.getLogger("org.springframework.transaction.support.DeadLetterFixture");
        Level previousLevel = databaseLogger.getLevel();
        databaseLogger.setLevel(Level.ERROR);
        try {
            Mockito.when(this.platformTransactionManager.getTransaction(Mockito.any())).thenAnswer(_ -> {
                Assertions.assertThat(databaseLogger.isErrorEnabled()).as("transaction begin").isFalse();
                return this.transactionStatus;
            });
            Mockito.when(this.deadLetterOccurrenceWriter.insertIfAbsent(Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.any(), Mockito.any())).thenAnswer(_ -> {
                Assertions.assertThat(databaseLogger.isErrorEnabled()).as("insert").isFalse();
                if (failsAtWrite) {
                    throw new IllegalStateException("raw transaction detail");
                }
                return 1;
            });
            if (failsAtWrite) {
                Mockito.doAnswer(_ -> {
                    Assertions.assertThat(databaseLogger.isErrorEnabled()).as("rollback").isFalse();
                    return null;
                }).when(this.platformTransactionManager).rollback(this.transactionStatus);
                assertTransactionFailure(() -> this.deadLetterOccurrenceService.persistIfAbsent(storedWebhookEnvelope, DEAD_LETTERED_AT), "unknown");
                Mockito.verify(this.platformTransactionManager).rollback(this.transactionStatus);
            } else {
                Mockito.doAnswer(_ -> {
                    Assertions.assertThat(databaseLogger.isErrorEnabled()).as("commit").isFalse();
                    return null;
                }).when(this.platformTransactionManager).commit(this.transactionStatus);
                this.deadLetterOccurrenceService.persistIfAbsent(storedWebhookEnvelope, DEAD_LETTERED_AT);
                Mockito.verify(this.platformTransactionManager).commit(this.transactionStatus);
            }
            Assertions.assertThat(databaseLogger.isErrorEnabled()).isTrue();
        } finally {
            databaseLogger.setLevel(previousLevel);
        }
    }

    @Test
    void rejectsMissingTimestampBeforeDecodingOrOpeningATransaction() {
        Assertions.assertThatIllegalArgumentException().isThrownBy(() -> this.deadLetterOccurrenceService.persistIfAbsent(null, null)).withMessage("Dead-letter timestamp is required").withNoCause();
        Mockito.verifyNoInteractions(this.platformTransactionManager, this.deadLetterOccurrenceWriter);
    }

    @ParameterizedTest(name = "{2}")
    @MethodSource("invalidEnvelopes")
    void rejectsInvalidEnvelopesBeforeOpeningATransaction(StoredWebhookEnvelope storedWebhookEnvelope, Class<? extends RuntimeException> exceptionType, String expectedMessage) {
        Assertions.assertThatThrownBy(() -> this.deadLetterOccurrenceService.persistIfAbsent(storedWebhookEnvelope, DEAD_LETTERED_AT))
                .isExactlyInstanceOf(exceptionType)
                .hasMessage(expectedMessage)
                .hasNoCause()
                .satisfies(exception -> Assertions.assertThat(exception.getSuppressed()).isEmpty());
        Mockito.verifyNoInteractions(this.platformTransactionManager, this.deadLetterOccurrenceWriter);
    }

    private static Stream<Arguments> invalidEnvelopes() throws IOException {
        return Stream.of(
                Arguments.of(null, InvalidWebhookEnvelopeException.class, "Webhook envelope is missing"),
                Arguments.of(new StoredWebhookEnvelope("{}", new byte[]{1}), InvalidWebhookEnvelopeException.class, "Webhook envelope writer schema is invalid"),
                Arguments.of(new StoredWebhookEnvelope(WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA.toString(), new byte[]{1}), InvalidWebhookEnvelopeException.class, "Webhook envelope datum is malformed"),
                Arguments.of(olderEnvelope("1-1-1-1-1"), IllegalArgumentException.class, "Webhook envelope event ID must be a UUID"),
                Arguments.of(new WebhookEnvelopeCodec().encode(WebhookEnvelopeFixtures.currentEvent(EVENT_ID.toString(), -1, null)), IllegalArgumentException.class, "Dead-letter replay count must be nonnegative")
        );
    }

    private static StoredWebhookEnvelope olderEnvelope() throws IOException {
        return olderEnvelope(EVENT_ID.toString().toUpperCase(Locale.ROOT));
    }

    private static StoredWebhookEnvelope olderEnvelope(String eventIdText) throws IOException {
        return new StoredWebhookEnvelope(
                WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA.toString(),
                WebhookEnvelopeFixtures.writeDatum(WebhookEnvelopeFixtures.phaseOneEvent(WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA, eventIdText))
        );
    }

    private static void assertTransactionFailure(Runnable runnable, String sqlState) {
        Assertions.assertThatIllegalStateException()
                .isThrownBy(runnable::run)
                .withMessage("Dead-letter occurrence transaction failed [sqlState=" + sqlState + "]")
                .withNoCause()
                .satisfies(exception -> Assertions.assertThat(exception.getSuppressed()).isEmpty());
    }

    private enum FailurePhase {
        BEGIN, WRITE, COMMIT_INSERTED, COMMIT_DUPLICATE
    }
}
