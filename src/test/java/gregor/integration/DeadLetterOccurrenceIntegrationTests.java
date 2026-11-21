package gregor.integration;

import gregor.avro.WebhookEnvelopeCodec;
import gregor.avro.WebhookEnvelopeFixtures;
import gregor.avro.WebhookEvent;
import gregor.dto.StoredWebhookEnvelope;
import gregor.entity.DeadLetterOccurrence;
import gregor.exception.InvalidWebhookEnvelopeException;
import gregor.repository.DeadLetterOccurrenceRepository;
import gregor.service.DeadLetterOccurrenceService;
import jakarta.persistence.EntityExistsException;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.PersistenceException;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.type.BasicType;
import org.hibernate.type.descriptor.ValueBinder;
import org.hibernate.type.descriptor.java.JavaType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.UnexpectedRollbackException;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.ByteBuffer;
import java.sql.CallableStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.stream.Stream;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.datasource.url=jdbc:postgresql://${POSTGRES_HOST:localhost}:${POSTGRES_PORT:5432}/${POSTGRES_DB:gregor}",
                "spring.datasource.username=${POSTGRES_USER:gregor}",
                "spring.datasource.password=${POSTGRES_PASSWORD:gregor}",
                "spring.jpa.hibernate.ddl-auto=update"
        }
)
class DeadLetterOccurrenceIntegrationTests {

    private static final Instant DEAD_LETTERED_AT = Instant.parse("2026-07-10T12:00:01.987Z");
    private static final String RAW_INSERT = """
                                             INSERT INTO dead_letter_occurrence (occurrence_id, event_id, replay_count, writer_schema, datum_bytes, dead_lettered_at)
                                             VALUES (?, ?, ?, ?, ?, ?)
                                             """;

    @Autowired
    private DeadLetterOccurrenceService deadLetterOccurrenceService;
    @Autowired
    private DeadLetterOccurrenceRepository deadLetterOccurrenceRepository;
    @Autowired
    private WebhookEnvelopeCodec webhookEnvelopeCodec;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private EntityManagerFactory entityManagerFactory;
    @Autowired
    private PlatformTransactionManager platformTransactionManager;

    @Test
    void storesDerivedIdentityEnvelopeAndSuppliedTimestampAtMillisecondPrecision() {
        UUID occurrenceId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        String eventIdText = eventId.toString().toUpperCase(Locale.ROOT);
        WebhookEvent webhookEvent = WebhookEnvelopeFixtures.currentEvent(eventIdText, 2, WebhookEnvelopeFixtures.NEXT_ELIGIBLE_AT);
        StoredWebhookEnvelope storedWebhookEnvelope = this.webhookEnvelopeCodec.encode(webhookEvent);

        DeadLetterOccurrence insertedDeadLetterOccurrence = this.deadLetterOccurrenceService.insert(occurrenceId, storedWebhookEnvelope, Instant.parse("2026-07-10T12:00:01.987654Z"));

        assertOccurrence(insertedDeadLetterOccurrence, occurrenceId, eventId, 2, DEAD_LETTERED_AT, false, storedWebhookEnvelope);
        DeadLetterOccurrence deadLetterOccurrence = this.deadLetterOccurrenceRepository.findById(occurrenceId).orElseThrow();
        assertOccurrence(deadLetterOccurrence, occurrenceId, eventId, 2, DEAD_LETTERED_AT, false, storedWebhookEnvelope);
        Assertions.assertThat(this.webhookEnvelopeCodec.decode(deadLetterOccurrence.getStoredWebhookEnvelope())).isEqualTo(webhookEvent);
        Instant persistedDeadLetteredAt = this.jdbcTemplate.queryForObject(
                """
                SELECT dead_lettered_at FROM dead_letter_occurrence WHERE occurrence_id = ?
                """,
                (resultSet, _) -> resultSet.getObject(1, OffsetDateTime.class).toInstant(),
                occurrenceId
        );
        Assertions.assertThat(persistedDeadLetteredAt).isEqualTo(DEAD_LETTERED_AT);
        Assertions.assertThat(this.jdbcTemplate.queryForObject(
                """
                SELECT datum_bytes FROM dead_letter_occurrence WHERE occurrence_id = ?
                """,
                byte[].class,
                occurrenceId
        )).containsExactly(storedWebhookEnvelope.datumBytes());
        Assertions.assertThat(this.jdbcTemplate.queryForObject(
                """
                SELECT writer_schema FROM dead_letter_occurrence WHERE occurrence_id = ?
                """,
                String.class,
                occurrenceId
        )).isEqualTo(storedWebhookEnvelope.writerSchemaJson());
    }

    @Test
    void exposesOnlyTheRequiredPostgresqlColumnsAndTypes() {
        Assertions.assertThat(this.jdbcTemplate.queryForList(
                        """
                        SELECT column_name, data_type, datetime_precision, is_nullable
                        FROM information_schema.columns
                        WHERE table_schema = current_schema() AND table_name = 'dead_letter_occurrence'
                        """
                ))
                .extracting(columns -> Assertions.tuple(columns.get("column_name"), columns.get("data_type"), columns.get("datetime_precision"), columns.get("is_nullable")))
                .containsExactlyInAnyOrder(
                        Assertions.tuple("occurrence_id", "uuid", null, "NO"),
                        Assertions.tuple("event_id", "uuid", null, "NO"),
                        Assertions.tuple("replay_count", "integer", null, "NO"),
                        Assertions.tuple("writer_schema", "text", null, "NO"),
                        Assertions.tuple("datum_bytes", "bytea", null, "NO"),
                        Assertions.tuple("dead_lettered_at", "timestamp with time zone", 3, "NO"),
                        Assertions.tuple("replay_accepted", "boolean", null, "NO")
                );
    }

    @Test
    void redactsPersistedEnvelopeValuesInHibernateDiagnostics() {
        SessionFactoryImplementor sessionFactoryImplementor = this.entityManagerFactory.unwrap(SessionFactoryImplementor.class);
        EntityPersister entityPersister = sessionFactoryImplementor.getMappingMetamodel().getEntityDescriptor(DeadLetterOccurrence.class);
        UUID eventId = UUID.randomUUID();
        StoredWebhookEnvelope storedWebhookEnvelope = this.encode(eventId, 2);

        Assertions.assertThat(loggableRepresentation(entityPersister.findAttributeMapping("eventId").getJavaType(), eventId)).isEqualTo("[redacted]");
        Assertions.assertThat(loggableRepresentation(entityPersister.findAttributeMapping("replayCount").getJavaType(), 2)).isEqualTo("[redacted]");
        Assertions.assertThat(loggableRepresentation(entityPersister.findAttributeMapping("writerSchemaJson").getJavaType(), storedWebhookEnvelope.writerSchemaJson())).isEqualTo("[redacted]");
        Assertions.assertThat(loggableRepresentation(entityPersister.findAttributeMapping("datumBytes").getJavaType(), storedWebhookEnvelope.datumBytes())).isEqualTo("[redacted]");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("envelopeBindings")
    void rejectsEnvelopeBindFailuresWithoutExposingBoundValues(String propertyName, Object value, String expectedMessage) {
        SessionFactoryImplementor sessionFactoryImplementor = this.entityManagerFactory.unwrap(SessionFactoryImplementor.class);
        EntityPersister entityPersister = sessionFactoryImplementor.getMappingMetamodel().getEntityDescriptor(DeadLetterOccurrence.class);
        BasicType<?> basicType = (BasicType<?>) entityPersister.findAttributeMapping(propertyName).getSingleJdbcMapping();
        assertBindFailures(basicType, propertyName, value, expectedMessage);
    }

    private static <T> void assertBindFailures(BasicType<T> basicType, String propertyName, Object value, String expectedMessage) {
        ValueBinder<T> valueBinder = basicType.getJdbcValueBinder();
        T boundValue = basicType.getJavaTypeDescriptor().cast(value);
        String driverDetail = boundValue instanceof byte[] datumBytes ? HexFormat.of().formatHex(datumBytes) : boundValue.toString();
        CallableStatement callableStatement = Mockito.mock(
                CallableStatement.class,
                _ -> {
                    throw new SQLException("Could not bind " + driverDetail);
                }
        );

        List<ThrowingCallable> bindCalls = List.of(() -> valueBinder.bind(callableStatement, boundValue, 1, null), () -> valueBinder.bind(callableStatement, boundValue, propertyName, null));
        for (ThrowingCallable bindCall : bindCalls) {
            Assertions.assertThatExceptionOfType(PersistenceException.class).isThrownBy(bindCall).withMessage(expectedMessage).withNoCause().satisfies(exception -> Assertions.assertThat(exception.getSuppressed()).isEmpty());
        }
    }

    private static Stream<Arguments> envelopeBindings() {
        return Stream.of(
                Arguments.of("eventId", UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), "Dead-letter occurrence event ID could not be bound"),
                Arguments.of("replayCount", 971, "Dead-letter occurrence replay count could not be bound"),
                Arguments.of(
                        "writerSchemaJson",
                        WebhookEnvelopeFixtures.phaseOneSchemaJsonWith("{\"name\":\"writerOnly\",\"type\":\"null\",\"doc\":\"private schema detail\"}"),
                        "Dead-letter occurrence writer schema could not be bound"
                ),
                Arguments.of("datumBytes", new byte[]{0x7b, 0x20, 0x7d}, "Dead-letter occurrence datum could not be bound")
        );
    }

    @Test
    void conflictsRollBackWithoutReplacingEitherExistingOccurrence() {
        UUID eventId = UUID.randomUUID();
        UUID lowestOccurrenceId = UUID.randomUUID();
        UUID highestOccurrenceId = UUID.randomUUID();
        UUID unusedOccurrenceId = UUID.randomUUID();
        StoredWebhookEnvelope lowestStoredWebhookEnvelope = this.encode(eventId, 0);
        StoredWebhookEnvelope highestStoredWebhookEnvelope = this.encode(eventId, 2);
        Instant oldDeadLetteredAt = DEAD_LETTERED_AT.minusSeconds(10);
        this.deadLetterOccurrenceService.insert(lowestOccurrenceId, lowestStoredWebhookEnvelope, DEAD_LETTERED_AT);
        this.deadLetterOccurrenceService.insert(highestOccurrenceId, highestStoredWebhookEnvelope, oldDeadLetteredAt);
        this.jdbcTemplate.update("UPDATE dead_letter_occurrence SET replay_accepted = true WHERE occurrence_id = ?", highestOccurrenceId);

        StoredWebhookEnvelope middleStoredWebhookEnvelope = this.encode(eventId, 1);
        StoredWebhookEnvelope conflictingStoredWebhookEnvelope = this.webhookEnvelopeCodec.encode(WebhookEnvelopeFixtures.currentEvent(eventId.toString(), 2, WebhookEnvelopeFixtures.NEXT_ELIGIBLE_AT));
        UUID priorOccurrenceId = UUID.randomUUID();
        this.assertConflictRollsBack(
                priorOccurrenceId,
                () -> this.deadLetterOccurrenceService.insert(priorOccurrenceId, this.encode(eventId, 3), DEAD_LETTERED_AT),
                () -> this.deadLetterOccurrenceService.insert(lowestOccurrenceId, middleStoredWebhookEnvelope, oldDeadLetteredAt)
        );

        UUID otherPriorOccurrenceId = UUID.randomUUID();
        this.assertConflictRollsBack(
                otherPriorOccurrenceId,
                () -> this.deadLetterOccurrenceService.insert(otherPriorOccurrenceId, this.encode(eventId, 3), DEAD_LETTERED_AT),
                () -> this.deadLetterOccurrenceService.insert(unusedOccurrenceId, conflictingStoredWebhookEnvelope, DEAD_LETTERED_AT)
        );

        assertOccurrence(this.deadLetterOccurrenceRepository.findById(lowestOccurrenceId).orElseThrow(), lowestOccurrenceId, eventId, 0, DEAD_LETTERED_AT, false, lowestStoredWebhookEnvelope);
        assertOccurrence(this.deadLetterOccurrenceRepository.findById(highestOccurrenceId).orElseThrow(), highestOccurrenceId, eventId, 2, oldDeadLetteredAt, true, highestStoredWebhookEnvelope);
        Assertions.assertThat(this.deadLetterOccurrenceRepository.findById(unusedOccurrenceId)).isEmpty();
        Assertions.assertThat(this.deadLetterOccurrenceRepository.findByEventIdAndReplayCount(eventId, 1)).isEmpty();
        Assertions.assertThat(this.countOccurrences(eventId)).isEqualTo(2);
    }

    @Test
    void managedDuplicateIdReportsTheSameRedactedConflictAndRollsBack() {
        UUID eventId = UUID.randomUUID();
        UUID occurrenceId = UUID.randomUUID();
        UUID otherOccurrenceId = UUID.randomUUID();
        StoredWebhookEnvelope storedWebhookEnvelope = this.encode(eventId, 0);
        StoredWebhookEnvelope otherStoredWebhookEnvelope = this.encode(eventId, 1);
        StoredWebhookEnvelope conflictingStoredWebhookEnvelope = this.encode(eventId, 2);
        this.deadLetterOccurrenceService.insert(occurrenceId, storedWebhookEnvelope, DEAD_LETTERED_AT);

        this.assertConflictRollsBack(
                otherOccurrenceId,
                () -> {
                    DeadLetterOccurrence deadLetterOccurrence = this.deadLetterOccurrenceRepository.findById(occurrenceId).orElseThrow();
                    assertOccurrence(deadLetterOccurrence, occurrenceId, eventId, 0, DEAD_LETTERED_AT, false, storedWebhookEnvelope);
                    this.deadLetterOccurrenceService.insert(otherOccurrenceId, otherStoredWebhookEnvelope, DEAD_LETTERED_AT);
                },
                () -> this.deadLetterOccurrenceService.insert(occurrenceId, conflictingStoredWebhookEnvelope, DEAD_LETTERED_AT.minusSeconds(10))
        );

        assertOccurrence(this.deadLetterOccurrenceRepository.findById(occurrenceId).orElseThrow(), occurrenceId, eventId, 0, DEAD_LETTERED_AT, false, storedWebhookEnvelope);
        Assertions.assertThat(this.countOccurrences(eventId)).isEqualTo(1);
    }

    @ParameterizedTest
    @CsvSource({
            "true, true, true",
            "false, true, true",
            "true, false, true",
            "true, true, false"
    })
    void latestLookupUsesGenerationRatherThanTimeInsertionOrderOrAcceptance(boolean highestTimestampEarlier, boolean highestInsertedFirst, boolean highestReplayAccepted) {
        UUID eventId = UUID.randomUUID();
        UUID otherEventId = UUID.randomUUID();
        UUID highestOccurrenceId = UUID.randomUUID();
        UUID lowestOccurrenceId = UUID.randomUUID();
        UUID otherOccurrenceId = UUID.randomUUID();
        StoredWebhookEnvelope highestStoredWebhookEnvelope = this.encode(eventId, 2);
        Instant nextEligibleAt = Instant.parse("2026-07-10T12:00:00Z");
        Instant lowestDeadLetteredAt = Instant.parse("2026-07-10T12:00:01Z");
        Instant highestDeadLetteredAt = highestTimestampEarlier ? Instant.parse("2001-01-01T00:00:00.123Z") : lowestDeadLetteredAt.plusSeconds(1);
        StoredWebhookEnvelope lowestStoredWebhookEnvelope = this.webhookEnvelopeCodec.encode(WebhookEnvelopeFixtures.currentEvent(eventId.toString(), 0, nextEligibleAt));
        if (highestInsertedFirst) {
            this.deadLetterOccurrenceService.insert(highestOccurrenceId, highestStoredWebhookEnvelope, highestDeadLetteredAt);
        }
        this.deadLetterOccurrenceService.insert(lowestOccurrenceId, lowestStoredWebhookEnvelope, lowestDeadLetteredAt);
        if (!highestInsertedFirst) {
            this.deadLetterOccurrenceService.insert(highestOccurrenceId, highestStoredWebhookEnvelope, highestDeadLetteredAt);
        }
        this.deadLetterOccurrenceService.insert(otherOccurrenceId, this.encode(otherEventId, 0), DEAD_LETTERED_AT);
        UUID acceptedOccurrenceId = highestReplayAccepted ? highestOccurrenceId : lowestOccurrenceId;
        this.jdbcTemplate.update("UPDATE dead_letter_occurrence SET replay_accepted = true WHERE occurrence_id = ?", acceptedOccurrenceId);

        DeadLetterOccurrence highestDeadLetterOccurrence = this.deadLetterOccurrenceRepository.findFirstByEventIdOrderByReplayCountDesc(eventId).orElseThrow();
        assertOccurrence(highestDeadLetterOccurrence, highestOccurrenceId, eventId, 2, highestDeadLetteredAt, highestReplayAccepted, highestStoredWebhookEnvelope);
        DeadLetterOccurrence lowestDeadLetterOccurrence = this.deadLetterOccurrenceRepository.findByEventIdAndReplayCount(eventId, 0).orElseThrow();
        assertOccurrence(lowestDeadLetterOccurrence, lowestOccurrenceId, eventId, 0, lowestDeadLetteredAt, !highestReplayAccepted, lowestStoredWebhookEnvelope);
        DeadLetterOccurrence lowestByIdDeadLetterOccurrence = this.deadLetterOccurrenceRepository.findById(lowestOccurrenceId).orElseThrow();
        assertOccurrence(lowestByIdDeadLetterOccurrence, lowestOccurrenceId, eventId, 0, lowestDeadLetteredAt, !highestReplayAccepted, lowestStoredWebhookEnvelope);
        WebhookEvent webhookEvent = this.webhookEnvelopeCodec.decode(lowestDeadLetterOccurrence.getStoredWebhookEnvelope());
        Assertions.assertThat(webhookEvent.getDeliveryAttemptCount()).isEqualTo(4);
        Assertions.assertThat(webhookEvent.getRetryCount()).isEqualTo(3);
        Assertions.assertThat(webhookEvent.getReplayCount()).isZero();
        Assertions.assertThat(webhookEvent.getNextEligibleAt()).isEqualTo(nextEligibleAt);
        Assertions.assertThat(webhookEvent.getPayload()).isEqualTo(ByteBuffer.wrap(new byte[]{0x7b, 0x20, 0x7d}));
        Assertions.assertThat(this.deadLetterOccurrenceRepository.findByEventIdAndReplayCount(eventId, 2)).get().extracting(DeadLetterOccurrence::getOccurrenceId).isEqualTo(highestOccurrenceId);
        Assertions.assertThat(this.deadLetterOccurrenceRepository.findByEventIdAndReplayCount(otherEventId, 0)).get().extracting(DeadLetterOccurrence::getOccurrenceId).isEqualTo(otherOccurrenceId);
        Assertions.assertThat(this.deadLetterOccurrenceRepository.findFirstByEventIdOrderByReplayCountDesc(otherEventId)).get().extracting(DeadLetterOccurrence::getOccurrenceId).isEqualTo(otherOccurrenceId);
        UUID unknownId = UUID.randomUUID();
        Assertions.assertThat(this.deadLetterOccurrenceRepository.findById(unknownId)).isEmpty();
        Assertions.assertThat(this.deadLetterOccurrenceRepository.findByEventIdAndReplayCount(eventId, 1)).isEmpty();
        Assertions.assertThat(this.deadLetterOccurrenceRepository.findByEventIdAndReplayCount(unknownId, 0)).isEmpty();
        Assertions.assertThat(this.deadLetterOccurrenceRepository.findFirstByEventIdOrderByReplayCountDesc(unknownId)).isEmpty();
    }

    @Test
    void rejectsMissingInputsEqualIdsAndNegativeGenerationBeforeWriting() {
        UUID eventId = UUID.randomUUID();
        UUID occurrenceId = UUID.randomUUID();
        StoredWebhookEnvelope storedWebhookEnvelope = this.encode(eventId, 0);
        StoredWebhookEnvelope negativeStoredWebhookEnvelope = this.encode(eventId, -1);
        assertInvalidInput(() -> this.deadLetterOccurrenceService.insert(null, storedWebhookEnvelope, DEAD_LETTERED_AT), "Dead-letter occurrence ID is required");
        assertInvalidInput(() -> this.deadLetterOccurrenceService.insert(occurrenceId, storedWebhookEnvelope, null), "Dead-letter timestamp is required");
        assertInvalidInput(() -> this.deadLetterOccurrenceService.insert(eventId, storedWebhookEnvelope, DEAD_LETTERED_AT), "Dead-letter occurrence ID must differ from the event ID");
        assertInvalidInput(() -> this.deadLetterOccurrenceService.insert(occurrenceId, negativeStoredWebhookEnvelope, DEAD_LETTERED_AT), "Dead-letter replay count must be nonnegative");
        Assertions.assertThat(this.countOccurrences(eventId)).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = {"private-not-a-uuid", "1-1-1-1-1"})
    void rejectsInvalidAndNoncanonicalEnvelopeEventIds(String eventIdText) {
        UUID occurrenceId = UUID.randomUUID();
        StoredWebhookEnvelope storedWebhookEnvelope = this.webhookEnvelopeCodec.encode(WebhookEnvelopeFixtures.currentEvent(eventIdText, 0, null));
        assertInvalidInput(() -> this.deadLetterOccurrenceService.insert(occurrenceId, storedWebhookEnvelope, DEAD_LETTERED_AT), "Webhook envelope event ID must be a UUID");
        Assertions.assertThat(this.deadLetterOccurrenceRepository.findById(occurrenceId)).isEmpty();
    }

    @Test
    void postgresqlRejectsEqualIdsAndNegativeGenerationEvenWhenInsertionBypassesTheService() {
        UUID eventId = UUID.randomUUID();
        UUID occurrenceId = UUID.randomUUID();
        StoredWebhookEnvelope placeholderStoredWebhookEnvelope = new StoredWebhookEnvelope("{}", new byte[]{1});
        assertSqlState(() -> this.insertRaw(eventId, eventId, 0, placeholderStoredWebhookEnvelope), "23514");
        assertSqlState(() -> this.insertRaw(occurrenceId, eventId, -1, placeholderStoredWebhookEnvelope), "23514");
        Assertions.assertThat(this.countOccurrences(eventId)).isZero();
    }

    @Test
    void rejectsInvalidWriterSchemaBeforePersisting() {
        UUID eventId = UUID.randomUUID();
        UUID occurrenceId = UUID.randomUUID();
        StoredWebhookEnvelope validStoredWebhookEnvelope = this.encode(eventId, 0);
        StoredWebhookEnvelope invalidStoredWebhookEnvelope = new StoredWebhookEnvelope(validStoredWebhookEnvelope.writerSchemaJson() + "\013", validStoredWebhookEnvelope.datumBytes());

        Assertions.assertThatExceptionOfType(InvalidWebhookEnvelopeException.class)
                .isThrownBy(() -> this.deadLetterOccurrenceService.insert(occurrenceId, invalidStoredWebhookEnvelope, DEAD_LETTERED_AT))
                .withMessage("Webhook envelope writer schema is invalid")
                .withNoCause();
        Assertions.assertThat(this.deadLetterOccurrenceRepository.findById(occurrenceId)).isEmpty();
    }

    @Test
    void malformedEnvelopeFailsExplicitlyInsteadOfBecomingAMissingRow() {
        UUID eventId = UUID.randomUUID();
        UUID occurrenceId = UUID.randomUUID();
        StoredWebhookEnvelope storedWebhookEnvelope = new StoredWebhookEnvelope(WebhookEvent.getClassSchema().toString(), new byte[]{1});
        Assertions.assertThatExceptionOfType(InvalidWebhookEnvelopeException.class)
                .isThrownBy(() -> this.deadLetterOccurrenceService.insert(occurrenceId, storedWebhookEnvelope, DEAD_LETTERED_AT))
                .withMessage("Webhook envelope datum is malformed")
                .withNoCause();
        Assertions.assertThat(this.deadLetterOccurrenceRepository.findById(occurrenceId)).isEmpty();

        this.insertRaw(occurrenceId, eventId, 0, storedWebhookEnvelope);
        List<DeadLetterOccurrence> deadLetterOccurrences = List.of(
                this.deadLetterOccurrenceRepository.findById(occurrenceId).orElseThrow(),
                this.deadLetterOccurrenceRepository.findByEventIdAndReplayCount(eventId, 0).orElseThrow(),
                this.deadLetterOccurrenceRepository.findFirstByEventIdOrderByReplayCountDesc(eventId).orElseThrow()
        );
        for (DeadLetterOccurrence deadLetterOccurrence : deadLetterOccurrences) {
            assertOccurrence(deadLetterOccurrence, occurrenceId, eventId, 0, DEAD_LETTERED_AT, false, storedWebhookEnvelope);
            Assertions.assertThatExceptionOfType(InvalidWebhookEnvelopeException.class)
                    .isThrownBy(() -> this.webhookEnvelopeCodec.decode(deadLetterOccurrence.getStoredWebhookEnvelope()))
                    .withMessage("Webhook envelope datum is malformed")
                    .withNoCause();
        }
    }

    private StoredWebhookEnvelope encode(UUID eventId, int replayCount) {
        return this.webhookEnvelopeCodec.encode(WebhookEnvelopeFixtures.currentEvent(eventId.toString(), replayCount, null));
    }

    private void insertRaw(UUID occurrenceId, UUID eventId, int replayCount, StoredWebhookEnvelope storedWebhookEnvelope) {
        this.jdbcTemplate.update(RAW_INSERT, occurrenceId, eventId, replayCount, storedWebhookEnvelope.writerSchemaJson(), storedWebhookEnvelope.datumBytes(), OffsetDateTime.parse(DEAD_LETTERED_AT.toString()));
    }

    private int countOccurrences(UUID eventId) {
        return this.jdbcTemplate.queryForObject("SELECT count(*) FROM dead_letter_occurrence WHERE event_id = ?", Integer.class, eventId);
    }

    private static void assertOccurrence(
            DeadLetterOccurrence deadLetterOccurrence,
            UUID occurrenceId,
            UUID eventId,
            int replayCount,
            Instant deadLetteredAt,
            boolean replayAccepted,
            StoredWebhookEnvelope storedWebhookEnvelope
    ) {
        Assertions.assertThat(deadLetterOccurrence.getOccurrenceId()).isEqualTo(occurrenceId);
        Assertions.assertThat(deadLetterOccurrence.getEventId()).isEqualTo(eventId).isNotEqualTo(occurrenceId);
        Assertions.assertThat(deadLetterOccurrence.getReplayCount()).isEqualTo(replayCount);
        Assertions.assertThat(deadLetterOccurrence.getDeadLetteredAt()).isEqualTo(deadLetteredAt);
        Assertions.assertThat(deadLetterOccurrence.isReplayAccepted()).isEqualTo(replayAccepted);
        Assertions.assertThat(deadLetterOccurrence.getStoredWebhookEnvelope().writerSchemaJson()).isEqualTo(storedWebhookEnvelope.writerSchemaJson());
        Assertions.assertThat(deadLetterOccurrence.getStoredWebhookEnvelope().datumBytes()).containsExactly(storedWebhookEnvelope.datumBytes());
    }

    private static void assertInvalidInput(ThrowingCallable throwingCallable, String message) {
        Assertions.assertThatIllegalArgumentException().isThrownBy(throwingCallable).withMessage(message).withNoCause().satisfies(exception -> Assertions.assertThat(exception.getSuppressed()).isEmpty());
    }

    private static <T> String loggableRepresentation(JavaType<T> javaType, Object value) {
        return javaType.extractLoggableRepresentation(javaType.cast(value));
    }

    private void assertConflictRollsBack(UUID priorOccurrenceId, Runnable insertBeforeConflict, ThrowingCallable insertConflict) {
        TransactionTemplate transactionTemplate = new TransactionTemplate(this.platformTransactionManager);
        Assertions.assertThatExceptionOfType(UnexpectedRollbackException.class).isThrownBy(() -> transactionTemplate.executeWithoutResult(_ -> {
            insertBeforeConflict.run();
            Boolean priorOccurrenceExists = this.jdbcTemplate.queryForObject(
                    "SELECT EXISTS (SELECT 1 FROM dead_letter_occurrence WHERE occurrence_id = ?)", Boolean.class, priorOccurrenceId
            );
            Assertions.assertThat(priorOccurrenceExists).isTrue();
            Assertions.assertThatExceptionOfType(EntityExistsException.class)
                    .isThrownBy(insertConflict)
                    .withMessage("Dead-letter occurrence ID or generation already exists")
                    .withNoCause()
                    .satisfies(exception -> Assertions.assertThat(exception.getSuppressed()).isEmpty());
        }));
        Assertions.assertThat(this.deadLetterOccurrenceRepository.findById(priorOccurrenceId)).isEmpty();
    }

    private static void assertSqlState(ThrowingCallable throwingCallable, String sqlState) {
        Assertions.assertThatThrownBy(throwingCallable).satisfies(exception -> Assertions.assertThat(NestedExceptionUtils.getMostSpecificCause(exception))
                .isInstanceOfSatisfying(SQLException.class, sqlException -> Assertions.assertThat(sqlException.getSQLState()).isEqualTo(sqlState)));
    }
}
