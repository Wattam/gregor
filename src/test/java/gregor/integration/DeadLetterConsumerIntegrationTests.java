package gregor.integration;

import gregor.avro.WebhookEnvelopeCodec;
import gregor.avro.WebhookEnvelopeFixtures;
import gregor.dto.StoredWebhookEnvelope;
import gregor.entity.DeadLetterOccurrence;
import gregor.exception.InvalidWebhookEnvelopeException;
import gregor.repository.DeadLetterOccurrenceRepository;
import io.apicurio.registry.resolver.client.RegistryArtifactReference;
import io.apicurio.registry.resolver.client.RegistryClientFacade;
import io.apicurio.registry.resolver.client.RegistryVersionCoordinates;
import io.apicurio.registry.serde.kafka.headers.KafkaSerdeHeaders;
import io.vertx.core.Vertx;
import lombok.RequiredArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.ServiceStatus;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.component.kafka.DefaultKafkaClientFactory;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.apache.camel.model.Model;
import org.apache.camel.spi.RouteController;
import org.apache.camel.spi.Synchronization;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.apache.camel.test.spring.junit5.UseAdviceWith;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.assertj.core.api.Assertions;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@UseAdviceWith
@CamelSpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.config.location=file:./src/main/resources/application.yaml",
                "spring.kafka.bootstrap-servers=localhost:${KAFKA_PORT:9092}",
                "camel.main.auto-startup=false",
                "camel.component.kafka.brokers=localhost:${KAFKA_PORT:9092}",
                "camel.component.kafka.consumers-count=3",
                "camel.component.kafka.shutdown-timeout=1000"
        }
)
class DeadLetterConsumerIntegrationTests {

    private static final Instant RECORD_TIME = Instant.parse("2020-01-02T03:04:05.123Z");
    private static final Instant DUPLICATE_TIME = Instant.parse("2020-01-02T03:04:06.456Z");
    private static final String PAYLOAD_MARKER = "private-dead-letter-fixture-payload";
    private static final String SIGNING_MARKER = "whsec_private_dead_letter_fixture_signing_material";
    private static final String DRIVER_MARKER = "private-driver-dead-letter-insert-detail";
    private static final String INVALID_TIMESTAMP = "Dead-letter Kafka record timestamp must be nonnegative epoch milliseconds";
    private static final String MALFORMED_FRAMING = "Apicurio webhook envelope framing is malformed";
    private static final String INVALID_ID = "Webhook envelope event ID must be a UUID";

    @Autowired
    private CamelContext camelContext;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private DeadLetterOccurrenceRepository deadLetterOccurrenceRepository;
    @Autowired
    private Environment environment;
    @Autowired
    private Vertx registryVertx;

    private AdminClient adminClient;
    private KafkaProducer<String, byte[]> kafkaProducer;
    private RegistryClientFacade registryClientFacade;
    private RegistryVersionCoordinates currentCoordinates;
    private String topicName;
    private String consumerUri;
    private final BlockingQueue<Consumption> consumptions = new LinkedBlockingQueue<>();
    private final CountDownLatch followerReached = new CountDownLatch(1);
    private final CountDownLatch releaseFollower = new CountDownLatch(1);
    private volatile Position heldFollower;
    private volatile TimestampOverride timestampOverride;

    private void provisionIsolatedInput(TimestampType timestampType) throws Exception {
        this.adminClient = KafkaTestRecords.adminClient();
        this.topicName = "gregor-it-dead-letter-" + UUID.randomUUID();
        NewTopic newTopic = new NewTopic(this.topicName, 3, (short) 1).configs(Map.of(
                TopicConfig.RETENTION_MS_CONFIG, "-1",
                TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, timestampType.toString(),
                TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, Long.toString(Long.MAX_VALUE)
        ));
        CreateTopicsOptions createTopicsOptions = new CreateTopicsOptions().timeoutMs(10_000);
        this.adminClient.createTopics(List.of(newTopic), createTopicsOptions).all().get(15, TimeUnit.SECONDS);
        this.kafkaProducer = KafkaTestRecords.rawProducer();
        this.registryClientFacade = DeadLetterTestRecords.registry(this.environment, this.registryVertx);
        this.currentCoordinates = DeadLetterTestRecords.registerCurrent(this.registryClientFacade);

        // Change only the topic. Keep the production group, serde, offset policy and processing chain.
        Model model = this.camelContext.getCamelContextExtension().getContextPlugin(Model.class);
        String originalUri = model.getRouteDefinition(DeadLetterTestRecords.ROUTE_ID).getInput().getEndpointUri();
        String originalTopicUri = "kafka:" + DeadLetterTestRecords.GROUP_ID;
        Assertions.assertThat(originalUri).startsWith(originalTopicUri + "?");
        this.consumerUri = "kafka:" + this.topicName + originalUri.substring(originalTopicUri.length());
        AdviceWith.adviceWith(this.camelContext, DeadLetterTestRecords.ROUTE_ID, false, adviceWithRouteBuilder -> {
            adviceWithRouteBuilder.replaceFromWith(this.consumerUri);
            adviceWithRouteBuilder.weaveAddFirst().process(this::observeConsumption);
        });
    }

    @AfterEach
    void removeOnlyTheFixtureTopic() throws Exception {
        try (AdminClient _ = this.adminClient; KafkaProducer<String, byte[]> _ = this.kafkaProducer; AutoCloseable _ = this::deleteFixtureTopic) {
            this.releaseFollower.countDown();
            this.stopConsumer();
        }
    }

    private void deleteFixtureTopic() throws InterruptedException, ExecutionException, TimeoutException {
        if (Objects.isNull(this.topicName)) {
            return;
        }
        try {
            this.adminClient.deleteTopics(List.of(this.topicName)).all().get(10, TimeUnit.SECONDS);
        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof UnknownTopicOrPartitionException)) {
                throw exception;
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void beginsAtTheEarliestRetainedOffsetOrTheInRangeCommitOnStartup(boolean commitBeyondEnd) throws Exception {
        this.provisionIsolatedInput(TimestampType.CREATE_TIME);
        List<UUID> earlierEventIds = List.of(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        List<UUID> retainedEventIds = List.of(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        for (int partition = 0; partition < retainedEventIds.size(); partition++) {
            this.sendCurrent(earlierEventIds.get(partition), 0, partition, RECORD_TIME);
        }
        List<RecordMetadata> retainedRecords = List.of(
                this.sendCurrent(retainedEventIds.get(0), 0, 0, RECORD_TIME),
                this.sendCurrent(retainedEventIds.get(1), 0, 1, RECORD_TIME),
                this.sendCurrent(retainedEventIds.get(2), 0, 2, RECORD_TIME)
        );
        Position missingCommit = Position.of(retainedRecords.get(0));
        Position outOfRangeCommit = Position.of(retainedRecords.get(1));
        Position inRangeCommit = Position.of(retainedRecords.get(2));
        this.adminClient.deleteRecords(Map.of(
                missingCommit.topicPartition(), RecordsToDelete.beforeOffset(missingCommit.offset()),
                outOfRangeCommit.topicPartition(), RecordsToDelete.beforeOffset(outOfRangeCommit.offset())
        )).all().get(10, TimeUnit.SECONDS);
        long logEndOffset = KafkaTestRecords.endOffsets(this.adminClient, this.topicName).get(outOfRangeCommit.topicPartition());
        long seededOffset = commitBeyondEnd ? logEndOffset + 1 : outOfRangeCommit.offset() - 1;
        this.adminClient
                .alterConsumerGroupOffsets(DeadLetterTestRecords.GROUP_ID, Map.of(outOfRangeCommit.topicPartition(), new OffsetAndMetadata(seededOffset), inRangeCommit.topicPartition(), new OffsetAndMetadata(inRangeCommit.offset())))
                .all()
                .get(10, TimeUnit.SECONDS);

        Assertions.assertThat(this.committedOffset(missingCommit)).isEqualTo(-1L);
        Assertions.assertThat(this.earliestOffset(missingCommit)).isEqualTo(missingCommit.offset()).isPositive();
        Assertions.assertThat(this.earliestOffset(outOfRangeCommit)).isEqualTo(outOfRangeCommit.offset());
        Assertions.assertThat(this.committedOffset(outOfRangeCommit)).isEqualTo(seededOffset);
        Assertions.assertThat(this.earliestOffset(inRangeCommit)).isLessThan(this.committedOffset(inRangeCommit));
        Assertions.assertThat(this.committedOffset(inRangeCommit)).isEqualTo(inRangeCommit.offset());
        this.startConsumer();

        List<Consumption> expectedConsumptions = retainedRecords.stream().map(recordMetadata -> new Consumption(Position.of(recordMetadata), null)).toList();
        Assertions.assertThat(List.of(this.nextConsumption(), this.nextConsumption(), this.nextConsumption())).containsExactlyInAnyOrderElementsOf(expectedConsumptions);
        for (RecordMetadata recordMetadata : retainedRecords) {
            this.awaitCommit(recordMetadata);
        }
        for (UUID eventId : retainedEventIds) {
            Assertions.assertThat(this.occurrence(eventId, 0).deadLetteredAt()).isEqualTo(RECORD_TIME);
            Assertions.assertThat(this.generations(eventId)).containsExactly(0);
        }
        for (UUID eventId : earlierEventIds) {
            Assertions.assertThat(this.generations(eventId)).isEmpty();
        }

        UUID persistedEventId = retainedEventIds.getFirst();
        OccurrenceSnapshot occurrenceSnapshot = this.occurrence(persistedEventId, 0);
        this.stopConsumer();
        this.adminClient.deleteRecords(Map.of(missingCommit.topicPartition(), RecordsToDelete.beforeOffset(missingCommit.offset() + 1))).all().get(10, TimeUnit.SECONDS);
        Assertions.assertThat(this.earliestOffset(missingCommit)).isEqualTo(missingCommit.offset() + 1);
        this.startConsumer();
        this.awaitCommit(this.sendCurrent(UUID.randomUUID(), 0, 0, DUPLICATE_TIME));
        Assertions.assertThat(this.occurrence(persistedEventId, 0)).isEqualTo(occurrenceSnapshot);
    }

    @Test
    void storesTheBrokerAppendTimestampInsteadOfTheProducerTimestamp() throws Exception {
        this.provisionIsolatedInput(TimestampType.LOG_APPEND_TIME);
        UUID eventId = UUID.randomUUID();
        RecordMetadata inputRecord = this.sendCurrent(eventId, 0, 0, RECORD_TIME);
        Position inputPosition = Position.of(inputRecord);
        Instant brokerAppendedAt;
        try (KafkaConsumer<String, Object> kafkaConsumer = KafkaTestRecords.consumer(this.registryVertx, Map.of(inputPosition.topicPartition(), inputPosition.offset()))) {
            ConsumerRecord<String, Object> consumerRecord = KafkaTestRecords.awaitRecord(kafkaConsumer, eventId.toString());
            Assertions.assertThat(consumerRecord.offset()).isEqualTo(inputPosition.offset());
            Assertions.assertThat(consumerRecord.timestampType()).isEqualTo(TimestampType.LOG_APPEND_TIME);
            brokerAppendedAt = Instant.ofEpochMilli(consumerRecord.timestamp());
        }
        Assertions.assertThat(brokerAppendedAt).isAfter(RECORD_TIME);

        this.startConsumer();
        this.awaitCommit(inputRecord);

        Assertions.assertThat(this.occurrence(eventId, 0).deadLetteredAt()).isEqualTo(brokerAppendedAt);
    }

    @Test
    void preservesOlderWriterDataForPrefixAndHeadersAndResolvesPinnedReferences() throws Exception {
        this.provisionIsolatedInput(TimestampType.CREATE_TIME);
        String referenceSchemaJson = """
                                     {"type":"record","name":"gregor.avro.DeadLetterFixtureContext","fields":[{"name":"reason","type":"string"}]}
                                     """;
        RegistryVersionCoordinates referenceCoordinates = DeadLetterTestRecords.register(this.registryClientFacade, "context", referenceSchemaJson, Set.of());
        String referenceGroupId = Objects.requireNonNullElse(referenceCoordinates.getGroupId(), "default");
        RegistryArtifactReference registryArtifactReference = RegistryArtifactReference
                .builder()
                .name("gregor.avro.DeadLetterFixtureContext")
                .groupId(referenceGroupId)
                .artifactId(referenceCoordinates.getArtifactId())
                .version(referenceCoordinates.getVersion())
                .build();
        String writerSchemaJson = WebhookEnvelopeFixtures.phaseOneSchemaJsonWith(
                """
                {"name":"writerContext","type":"DeadLetterFixtureContext"}
                """
        );
        RegistryVersionCoordinates writerCoordinates = DeadLetterTestRecords.register(this.registryClientFacade, "original-writer", writerSchemaJson, Set.of(registryArtifactReference));
        RegistryVersionCoordinates newerReferenceCoordinates = DeadLetterTestRecords.register(
                this.registryClientFacade, "context", referenceSchemaJson.replace("\"reason\"", "\"laterReason\""), Set.of()
        );
        Assertions.assertThat(newerReferenceCoordinates.getVersion()).isNotEqualTo(referenceCoordinates.getVersion());
        RegistryVersionCoordinates latestReferenceCoordinates = this.registryClientFacade.getVersionCoordinatesByGAV(referenceGroupId, referenceCoordinates.getArtifactId(), null);
        Assertions.assertThat(latestReferenceCoordinates.getVersion()).isEqualTo(newerReferenceCoordinates.getVersion());
        Schema unreferencedSchema = WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA;
        RegistryVersionCoordinates hashCoordinates = DeadLetterTestRecords.register(this.registryClientFacade, "hash-writer", unreferencedSchema.toString(), Set.of());
        String registeredHashSchema = this.registryClientFacade.getSchemaByContentId(hashCoordinates.getContentId());
        byte[] hashBytes = MessageDigest.getInstance("SHA-256").digest(registeredHashSchema.getBytes(StandardCharsets.UTF_8));
        byte[] contentHashBytes = HexFormat.of().formatHex(hashBytes).getBytes(StandardCharsets.UTF_8);
        Schema referencedSchema = WebhookEnvelopeFixtures.phaseOneSchemaWith("{\"name\":\"writerContext\",\"type\":" + referenceSchemaJson + "}");
        this.startConsumer();

        for (Identifier identifier : Identifier.values()) {
            UUID eventId = UUID.randomUUID();
            boolean referencesContext = !Objects.equals(identifier, Identifier.CONTENT_HASH);
            Schema expectedSchema = referencesContext ? referencedSchema : unreferencedSchema;
            GenericRecord genericRecord = WebhookEnvelopeFixtures.phaseOneEvent(expectedSchema, eventId.toString());
            if (referencesContext) {
                GenericRecord contextGenericRecord = new GenericData.Record(expectedSchema.getField("writerContext").schema());
                contextGenericRecord.put("reason", "archived \uD83D\uDE80 context");
                genericRecord.put("writerContext", contextGenericRecord);
            }
            byte[] datumBytes = WebhookEnvelopeFixtures.writeDatum(genericRecord);
            Headers headers = switch (identifier) {
                case PREFIX -> new RecordHeaders();
                case CONTENT_ID -> DeadLetterTestRecords.idHeader(KafkaSerdeHeaders.HEADER_VALUE_CONTENT_ID, writerCoordinates.getContentId());
                case CONTENT_HASH -> new RecordHeaders().add(KafkaSerdeHeaders.HEADER_VALUE_CONTENT_HASH, contentHashBytes);
                case GLOBAL_ID -> DeadLetterTestRecords.idHeader(KafkaSerdeHeaders.HEADER_VALUE_GLOBAL_ID, writerCoordinates.getGlobalId());
            };
            headers.add("signing-secret", SIGNING_MARKER.getBytes(StandardCharsets.UTF_8));
            byte[] valueBytes = Objects.equals(identifier, Identifier.PREFIX) ? DeadLetterTestRecords.prefix(writerCoordinates, datumBytes) : datumBytes;
            this.awaitCommit(this.send(0, RECORD_TIME, eventId, valueBytes, headers));

            OccurrenceSnapshot occurrenceSnapshot = this.occurrence(eventId, 0);
            StoredWebhookEnvelope storedWebhookEnvelope = occurrenceSnapshot.storedWebhookEnvelope();
            Assertions.assertThat(occurrenceSnapshot.occurrenceId()).isNotNull().isNotEqualTo(eventId);
            Assertions.assertThat(occurrenceSnapshot.deadLetteredAt()).isEqualTo(RECORD_TIME);
            Assertions.assertThat(occurrenceSnapshot.replayAccepted()).isFalse();
            Assertions.assertThat(this.generations(eventId)).containsExactly(0);
            Assertions.assertThat(storedWebhookEnvelope.datumBytes()).as("unframed datum for %s", identifier).containsExactly(datumBytes);
            Assertions.assertThat(storedWebhookEnvelope.writerSchemaJson()).doesNotContain(SIGNING_MARKER, "signing-secret");
            Schema storedSchema = new Schema.Parser().parse(storedWebhookEnvelope.writerSchemaJson());
            Assertions.assertThat(storedSchema).isEqualTo(expectedSchema);
            BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(storedWebhookEnvelope.datumBytes(), null);
            GenericRecord retainedGenericRecord = new GenericDatumReader<GenericRecord>(storedSchema).read(null, binaryDecoder);
            Assertions.assertThat(retainedGenericRecord.get("payload")).isEqualTo(ByteBuffer.wrap(new byte[]{0x7b, 0x20, 0x7d}));
            if (referencesContext) {
                GenericRecord retainedContextGenericRecord = (GenericRecord) retainedGenericRecord.get("writerContext");
                Assertions.assertThat(retainedContextGenericRecord.get("reason").toString()).isEqualTo("archived \uD83D\uDE80 context");
            }
            Assertions.assertThat(binaryDecoder.isEnd()).isTrue();
            WebhookEnvelopeFixtures.assertPhaseOneEvent(new WebhookEnvelopeCodec().decode(storedWebhookEnvelope), eventId.toString());
        }
    }

    @ParameterizedTest
    @CsvSource({"2, 0", "0, 1"})
    void commitsChangedCopiesAndPersistsDistinctGenerationsInEitherOrder(int firstReplayCount, int nextReplayCount) throws Exception {
        this.provisionIsolatedInput(TimestampType.CREATE_TIME);
        UUID eventId = UUID.randomUUID();
        this.startConsumer();
        this.awaitCommit(this.sendCurrent(eventId, firstReplayCount, 0, RECORD_TIME));
        OccurrenceSnapshot firstOccurrence = this.acceptReplay(eventId, firstReplayCount);
        Assertions.assertThat(firstOccurrence.deadLetteredAt()).isEqualTo(RECORD_TIME);

        GenericRecord changedGenericRecord = DeadLetterTestRecords.terminalDatum(eventId.toString(), firstReplayCount, WebhookEnvelopeFixtures.NEXT_ELIGIBLE_AT);
        changedGenericRecord.put("payload", ByteBuffer.wrap(new byte[]{1, -1, 16}));
        this.awaitCommit(this.send(0, DUPLICATE_TIME, eventId, this.currentValue(changedGenericRecord), new RecordHeaders()));
        Assertions.assertThat(this.occurrence(eventId, firstReplayCount)).isEqualTo(firstOccurrence);
        Schema alternateSchema = WebhookEnvelopeFixtures.phaseOneSchemaWith(
                """
                {"name":"replayCount","type":"int"}, {"name":"legacyCode","type":"int"}
                """
        );
        GenericRecord alternateGenericRecord = WebhookEnvelopeFixtures.phaseOneEvent(alternateSchema, eventId.toString());
        alternateGenericRecord.put("replayCount", firstReplayCount);
        alternateGenericRecord.put("legacyCode", 73);
        RegistryVersionCoordinates coordinates = DeadLetterTestRecords.register(this.registryClientFacade, "alternate-writer", alternateSchema.toString(), Set.of());
        byte[] alternateValueBytes = DeadLetterTestRecords.prefix(coordinates, WebhookEnvelopeFixtures.writeDatum(alternateGenericRecord));
        this.awaitCommit(this.send(0, DUPLICATE_TIME, eventId, alternateValueBytes, new RecordHeaders()));
        Assertions.assertThat(this.occurrence(eventId, firstReplayCount)).isEqualTo(firstOccurrence);

        this.awaitCommit(this.sendCurrent(eventId, nextReplayCount, 0, DUPLICATE_TIME));
        OccurrenceSnapshot nextOccurrence = this.occurrence(eventId, nextReplayCount);
        Assertions.assertThat(nextOccurrence.occurrenceId()).isNotNull().isNotIn(eventId, firstOccurrence.occurrenceId());
        Assertions.assertThat(nextOccurrence.deadLetteredAt()).isEqualTo(DUPLICATE_TIME);
        Assertions.assertThat(nextOccurrence.replayAccepted()).isFalse();
        Assertions.assertThat(this.generations(eventId)).containsExactlyInAnyOrder(firstReplayCount, nextReplayCount);
        Assertions.assertThat(this.occurrence(eventId, firstReplayCount)).isEqualTo(firstOccurrence);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void keepsTheFirstCommittedInsertWhenCopiesOverlapAcrossPartitions(boolean failFirstCommit) throws Exception {
        this.provisionIsolatedInput(TimestampType.CREATE_TIME);
        UUID eventId = UUID.randomUUID();
        GenericRecord firstGenericRecord = DeadLetterTestRecords.terminalDatum(eventId.toString(), 1, null);
        byte[] firstDatumBytes = WebhookEnvelopeFixtures.writeDatum(firstGenericRecord);
        GenericRecord competingGenericRecord = DeadLetterTestRecords.terminalDatum(eventId.toString(), 1, null);
        competingGenericRecord.put("payload", ByteBuffer.wrap(new byte[]{2, 6, -1}));
        byte[] competingDatumBytes = WebhookEnvelopeFixtures.writeDatum(competingGenericRecord);
        RecordMetadata firstRecord;
        RecordMetadata competingRecord;
        this.startConsumer();
        this.awaitIndependentPartitionAssignments();
        try (
                AutoCloseable _ = failFirstCommit ? DeadLetterDatabaseFixtures.installTrigger(
                        this.jdbcTemplate,
                        eventId,
                        DeadLetterDatabaseFixtures.TriggerPhase.AT_COMMIT,
                        """
                        IF NEW.dead_lettered_at = '%s'::timestamptz THEN
                            RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'First fixture insert failed at commit';
                        END IF;
                        """.formatted(RECORD_TIME)
                ) : null;
                DeadLetterDatabaseFixtures.InsertGate insertGate = DeadLetterDatabaseFixtures.holdInsert(this.jdbcTemplate, eventId)
        ) {
            firstRecord = this.send(0, RECORD_TIME, eventId, DeadLetterTestRecords.prefix(this.currentCoordinates, firstDatumBytes), new RecordHeaders());
            insertGate.awaitPendingInsert();
            competingRecord = this.send(1, DUPLICATE_TIME, eventId, DeadLetterTestRecords.prefix(this.currentCoordinates, competingDatumBytes), new RecordHeaders());
            insertGate.awaitCompetingInsert();
            Assertions.assertThat(this.generations(eventId)).isEmpty();
            this.assertCommittedOffset(firstRecord, -1L);
            this.assertCommittedOffset(competingRecord, -1L);
        }

        this.awaitCommit(competingRecord);
        this.awaitCommit(firstRecord);
        if (failFirstCommit) {
            Assertions.assertThat(this.consumptions).anySatisfy(consumption -> assertFailure(
                    consumption,
                    Position.of(firstRecord),
                    IllegalStateException.class,
                    "Dead-letter occurrence transaction failed [sqlState=23514]"
            ));
        } else {
            Assertions.assertThat(List.of(this.nextConsumption(), this.nextConsumption()))
                    .containsExactlyInAnyOrder(new Consumption(Position.of(firstRecord), null), new Consumption(Position.of(competingRecord), null));
        }
        OccurrenceSnapshot occurrenceSnapshot = this.occurrence(eventId, 1);
        Assertions.assertThat(occurrenceSnapshot.deadLetteredAt()).isEqualTo(failFirstCommit ? DUPLICATE_TIME : RECORD_TIME);
        Assertions.assertThat(occurrenceSnapshot.storedWebhookEnvelope().datumBytes()).containsExactly(failFirstCommit ? competingDatumBytes : firstDatumBytes);
        Assertions.assertThat(this.generations(eventId)).containsExactly(1);
    }

    @Test
    void rejectsAnOccurrenceIdCollisionForANewGenerationWithoutCommitting() throws Exception {
        this.provisionIsolatedInput(TimestampType.CREATE_TIME);
        UUID eventId = UUID.randomUUID();
        this.startConsumer();
        RecordMetadata existingRecord = this.sendCurrent(eventId, 0, 0, RECORD_TIME);
        this.awaitCommit(existingRecord);
        this.nextConsumption();
        OccurrenceSnapshot existingOccurrence = this.occurrence(eventId, 0);
        try (
                AutoCloseable _ = DeadLetterDatabaseFixtures.installTrigger(
                        this.jdbcTemplate,
                        eventId,
                        DeadLetterDatabaseFixtures.TriggerPhase.BEFORE_INSERT,
                        "NEW.occurrence_id := '%s'::uuid;".formatted(existingOccurrence.occurrenceId())
                )
        ) {
            RecordMetadata conflictingRecord;
            try {
                conflictingRecord = this.sendCurrent(eventId, 1, 0, DUPLICATE_TIME);
                assertFailure(this.nextConsumption(), Position.of(conflictingRecord), IllegalStateException.class, "Dead-letter occurrence transaction failed [sqlState=23505]");
            } finally {
                this.stopConsumer();
            }
            this.assertCommittedOffset(conflictingRecord, existingRecord.offset() + 1);
            Assertions.assertThat(this.generations(eventId)).containsExactly(0);
            Assertions.assertThat(this.occurrence(eventId, 0)).isEqualTo(existingOccurrence);
        }
    }

    @ParameterizedTest
    @EnumSource(names = {"BEFORE_INSERT", "AT_COMMIT"})
    void redeliversAfterAWriteOrCommitFailureWithoutSkippingThePartition(DeadLetterDatabaseFixtures.TriggerPhase triggerPhase) throws Exception {
        this.provisionIsolatedInput(TimestampType.CREATE_TIME);
        UUID eventId = UUID.randomUUID();
        UUID followerEventId = UUID.randomUUID();
        GenericRecord genericRecord = DeadLetterTestRecords.terminalDatum(eventId.toString(), 0, null);
        genericRecord.put("payload", ByteBuffer.wrap(PAYLOAD_MARKER.getBytes(StandardCharsets.UTF_8)));
        byte[] datumBytes = WebhookEnvelopeFixtures.writeDatum(genericRecord);
        RecordMetadata failedRecord = this.send(0, RECORD_TIME, eventId, DeadLetterTestRecords.prefix(this.currentCoordinates, datumBytes), signedHeaders());
        RecordMetadata followerRecord = this.sendCurrent(followerEventId, 0, 0, DUPLICATE_TIME);
        Position failedPosition = Position.of(failedRecord);
        Map<TopicPartition, Long> endOffsets = this.applicationAndFixtureEndOffsets();
        String expectedMessage = "Dead-letter occurrence transaction failed [sqlState=23514]";
        try (
                AutoCloseable _ = DeadLetterDatabaseFixtures.installTrigger(
                        this.jdbcTemplate,
                        eventId,
                        triggerPhase,
                        """
                        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = '%s',
                            DETAIL = NEW.writer_schema || ' ' || encode(NEW.datum_bytes, 'hex') || ' ' || '%s';
                        """.formatted(DRIVER_MARKER, PAYLOAD_MARKER)
                )
        ) {
            this.startConsumer();
            try {
                for (int delivery = 0; delivery < 2; delivery++) {
                    assertFailure(this.nextConsumption(), failedPosition, IllegalStateException.class, expectedMessage);
                }
            } finally {
                this.stopConsumer();
            }
            this.assertCommittedOffset(failedRecord, -1L);
            Assertions.assertThat(this.generations(eventId)).isEmpty();
            Assertions.assertThat(this.generations(followerEventId)).isEmpty();
            Assertions.assertThat(this.applicationAndFixtureEndOffsets()).isEqualTo(endOffsets);
        }

        this.heldFollower = Position.of(followerRecord);
        this.startConsumer();
        try {
            this.awaitCommit(failedRecord);
            Assertions.assertThat(this.occurrence(eventId, 0).storedWebhookEnvelope().datumBytes()).containsExactly(datumBytes);
            Assertions.assertThat(this.generations(followerEventId)).isEmpty();
        } finally {
            this.releaseFollower.countDown();
        }
        this.awaitCommit(followerRecord);
        Assertions.assertThat(this.occurrence(followerEventId, 0).deadLetteredAt()).isEqualTo(DUPLICATE_TIME);
    }

    @Test
    void redeliversAnUncommittedRecordAfterSuspensionDuringAFailedDatabaseCommit() throws Exception {
        this.provisionIsolatedInput(TimestampType.CREATE_TIME);
        UUID eventId = UUID.randomUUID();
        UUID followerEventId = UUID.randomUUID();
        RecordMetadata failedRecord = this.sendCurrent(eventId, 0, 0, RECORD_TIME);
        RecordMetadata followerRecord = this.sendCurrent(followerEventId, 0, 0, DUPLICATE_TIME);
        Position failedPosition = Position.of(failedRecord);

        org.apache.camel.component.kafka.KafkaConsumer storageKafkaConsumer;
        try (DeadLetterDatabaseFixtures.InsertGate insertGate = DeadLetterDatabaseFixtures.holdInsert(this.jdbcTemplate, eventId, DeadLetterDatabaseFixtures.TriggerPhase.AT_COMMIT, "RAISE check_violation;")) {
            this.startConsumer();
            insertGate.awaitPendingInsert();
            this.assertCommittedOffset(failedRecord, -1L);
            storageKafkaConsumer = (org.apache.camel.component.kafka.KafkaConsumer) this.camelContext.getRoute(DeadLetterTestRecords.ROUTE_ID).getConsumer();
            storageKafkaConsumer.suspend();
            insertGate.release();

            assertFailure(this.nextConsumption(), failedPosition, IllegalStateException.class, "Dead-letter occurrence transaction failed [sqlState=23514]");
            awaitKafkaPause(storageKafkaConsumer);
            this.assertCommittedOffset(failedRecord, -1L);
            Assertions.assertThat(this.generations(eventId)).isEmpty();
            Assertions.assertThat(this.generations(followerEventId)).isEmpty();
        }

        storageKafkaConsumer.resume();
        Assertions.assertThat(this.nextConsumption()).isEqualTo(new Consumption(failedPosition, null));
        Assertions.assertThat(this.nextConsumption()).isEqualTo(new Consumption(Position.of(followerRecord), null));
        this.awaitCommit(followerRecord);
        Assertions.assertThat(this.occurrence(eventId, 0).deadLetteredAt()).isEqualTo(RECORD_TIME);
        Assertions.assertThat(this.occurrence(followerEventId, 0).deadLetteredAt()).isEqualTo(DUPLICATE_TIME);
    }

    @ParameterizedTest(name = "suspend during failed commit: {0}")
    @ValueSource(booleans = {false, true})
    void retriesTheRetainedRecordWhenABeyondEndCommitBecomesReadable(boolean suspendDuringCommit) throws Exception {
        this.provisionIsolatedInput(TimestampType.CREATE_TIME);
        UUID expiredEventId = UUID.randomUUID();
        UUID failedEventId = UUID.randomUUID();
        UUID followerEventId = UUID.randomUUID();
        UUID eventIdAtSeededOffset = UUID.randomUUID();
        this.sendCurrent(expiredEventId, 0, 0, RECORD_TIME);
        RecordMetadata failedRecord = this.sendCurrent(failedEventId, 0, 0, RECORD_TIME);
        Position failedPosition = Position.of(failedRecord);
        TopicPartition topicPartition = failedPosition.topicPartition();
        this.adminClient.deleteRecords(Map.of(topicPartition, RecordsToDelete.beforeOffset(failedPosition.offset()))).all().get(10, TimeUnit.SECONDS);
        long seededOffset = failedPosition.offset() + 2;
        this.adminClient.alterConsumerGroupOffsets(DeadLetterTestRecords.GROUP_ID, Map.of(topicPartition, new OffsetAndMetadata(seededOffset))).all().get(10, TimeUnit.SECONDS);
        Assertions.assertThat(this.earliestOffset(failedPosition)).isEqualTo(failedPosition.offset());
        Assertions.assertThat(KafkaTestRecords.endOffsets(this.adminClient, this.topicName).get(topicPartition)).isEqualTo(seededOffset - 1);

        RecordMetadata recordAtSeededOffset;
        String expectedMessage = "Dead-letter occurrence transaction failed [sqlState=23514]";
        try (DeadLetterDatabaseFixtures.InsertGate insertGate = DeadLetterDatabaseFixtures.holdInsert(this.jdbcTemplate, failedEventId, DeadLetterDatabaseFixtures.TriggerPhase.AT_COMMIT, "RAISE check_violation;")) {
            this.startConsumer();
            insertGate.awaitPendingInsert();
            this.assertCommittedOffset(failedRecord, seededOffset);
            org.apache.camel.component.kafka.KafkaConsumer storageKafkaConsumer = (org.apache.camel.component.kafka.KafkaConsumer) this.camelContext.getRoute(DeadLetterTestRecords.ROUTE_ID).getConsumer();
            if (suspendDuringCommit) {
                storageKafkaConsumer.suspend();
            }
            this.heldFollower = Position.of(this.sendCurrent(followerEventId, 0, 0, DUPLICATE_TIME));
            recordAtSeededOffset = this.sendCurrent(eventIdAtSeededOffset, 0, 0, DUPLICATE_TIME);
            Assertions.assertThat(recordAtSeededOffset.offset()).isEqualTo(seededOffset);
            insertGate.release();

            assertFailure(this.nextConsumption(), failedPosition, IllegalStateException.class, expectedMessage);
            if (suspendDuringCommit) {
                awaitKafkaPause(storageKafkaConsumer);
                storageKafkaConsumer.resume();
            }
            assertFailure(this.nextConsumption(), failedPosition, IllegalStateException.class, expectedMessage);
            this.assertCommittedOffset(failedRecord, seededOffset);
            Assertions.assertThat(this.generations(failedEventId)).isEmpty();
            Assertions.assertThat(this.generations(followerEventId)).isEmpty();
            Assertions.assertThat(this.generations(eventIdAtSeededOffset)).isEmpty();
        }

        try {
            Assertions.assertThat(this.followerReached.await(20, TimeUnit.SECONDS)).as("successor reached after recovery").isTrue();
            this.assertCommittedOffset(failedRecord, failedRecord.offset() + 1);
            Assertions.assertThat(this.occurrence(failedEventId, 0).deadLetteredAt()).isEqualTo(RECORD_TIME);
            Assertions.assertThat(this.generations(followerEventId)).isEmpty();
            Assertions.assertThat(this.generations(eventIdAtSeededOffset)).isEmpty();
        } finally {
            this.releaseFollower.countDown();
        }
        this.awaitCommit(recordAtSeededOffset);
        Assertions.assertThat(this.occurrence(followerEventId, 0).deadLetteredAt()).isEqualTo(DUPLICATE_TIME);
        Assertions.assertThat(this.occurrence(eventIdAtSeededOffset, 0).deadLetteredAt()).isEqualTo(DUPLICATE_TIME);
        Assertions.assertThat(this.generations(expiredEventId)).isEmpty();
    }

    @Test
    void resumesAnUnacknowledgedDurableOccurrenceAfterRestartWithoutRefreshingIt() throws Exception {
        this.provisionIsolatedInput(TimestampType.CREATE_TIME);
        UUID eventId = UUID.randomUUID();
        RecordMetadata inputRecord = this.sendCurrent(eventId, 0, 0, RECORD_TIME);
        Position inputPosition = Position.of(inputRecord);
        AtomicBoolean commitsInterrupted = new AtomicBoolean(true);
        this.camelContext.getEndpoint(this.consumerUri, KafkaEndpoint.class).setKafkaClientFactory(new DefaultKafkaClientFactory() {
            @Override
            public KafkaConsumer<Object, Object> getConsumer(Properties properties) {
                return new KafkaConsumer<>(properties) {
                    private volatile Thread pollingThread;

                    @Override
                    public ConsumerRecords<Object, Object> poll(Duration duration) {
                        this.pollingThread = Thread.currentThread();
                        return super.poll(duration);
                    }

                    @Override
                    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
                        Assertions.assertThat(Thread.currentThread()).isSameAs(this.pollingThread);
                        Assertions.assertThat(offsets).containsOnlyKeys(inputPosition.topicPartition());
                        Assertions.assertThat(offsets.get(inputPosition.topicPartition()).offset()).isEqualTo(inputPosition.offset() + 1);
                        if (commitsInterrupted.get()) {
                            throw new WakeupException();
                        }
                        super.commitSync(offsets, timeout);
                    }
                };
            }
        });
        this.startConsumer();
        Consumption failedConsumption = this.nextConsumption();
        Assertions.assertThat(failedConsumption.position()).isEqualTo(inputPosition);
        Assertions.assertThat(failedConsumption.failure()).isInstanceOf(WakeupException.class);
        this.stopConsumer();
        this.assertCommittedOffset(inputRecord, -1L);
        OccurrenceSnapshot committedOccurrence = this.acceptReplay(eventId, 0);
        Assertions.assertThat(committedOccurrence.deadLetteredAt()).isEqualTo(RECORD_TIME);

        this.consumptions.clear();
        commitsInterrupted.set(false);
        this.startConsumer();
        this.awaitCommit(inputRecord);
        Assertions.assertThat(this.nextConsumption()).isEqualTo(new Consumption(inputPosition, null));
        Assertions.assertThat(this.occurrence(eventId, 0)).isEqualTo(committedOccurrence);
        Assertions.assertThat(this.generations(eventId)).containsExactly(0);
    }

    @ParameterizedTest
    @EnumSource
    void rejectsInvalidRecordsWithoutWritingOrSkippingTheirSuccessor(InvalidRecord invalidRecord) throws Exception {
        this.provisionIsolatedInput(TimestampType.CREATE_TIME);
        UUID eventId = UUID.fromString("00000001-" + UUID.randomUUID().toString().substring(9));
        UUID followerEventId = UUID.randomUUID();
        OccurrenceSnapshot priorOccurrence = null;
        long priorCommittedOffset = -1L;
        if (Objects.equals(invalidRecord, InvalidRecord.MALFORMED_DUPLICATE)) {
            this.startConsumer();
            RecordMetadata priorRecord = this.sendCurrent(eventId, 0, 0, RECORD_TIME);
            this.awaitCommit(priorRecord);
            priorCommittedOffset = priorRecord.offset() + 1;
            this.nextConsumption();
            priorOccurrence = this.acceptReplay(eventId, 0);
            this.stopConsumer();
        }
        String envelopeEventId = switch (invalidRecord) {
            case NONCANONICAL_UUID -> "1-" + eventId.toString().substring(9);
            case INVALID_UUID -> "private-invalid-event-id";
            default -> eventId.toString();
        };
        int replayCount = Objects.equals(invalidRecord, InvalidRecord.NEGATIVE_REPLAY) ? -1 : 0;
        GenericRecord genericRecord = DeadLetterTestRecords.terminalDatum(envelopeEventId, replayCount, null);
        genericRecord.put("payload", ByteBuffer.wrap(PAYLOAD_MARKER.getBytes(StandardCharsets.UTF_8)));
        Headers headers = signedHeaders();
        byte[] datumBytes = WebhookEnvelopeFixtures.writeDatum(genericRecord);
        byte[] valueBytes = switch (invalidRecord) {
            case TOMBSTONE -> null;
            case MALFORMED_PREFIX -> new byte[]{0};
            case MALFORMED_DUPLICATE -> DeadLetterTestRecords.prefix(this.currentCoordinates, Arrays.copyOf(datumBytes, datumBytes.length + 1));
            case INCOMPATIBLE_WRITER -> {
                String schemaJson = genericRecord.getSchema().toString().replace("gregor.avro", "incompatible.deadletter.fixture");
                RegistryVersionCoordinates coordinates = DeadLetterTestRecords.register(this.registryClientFacade, "incompatible-writer", schemaJson, Set.of());
                yield DeadLetterTestRecords.prefix(coordinates, datumBytes);
            }
            case UNKNOWN_WRITER -> {
                DeadLetterTestRecords.idHeader(KafkaSerdeHeaders.HEADER_VALUE_GLOBAL_ID, Long.MAX_VALUE).forEach(headers::add);
                yield datumBytes;
            }
            case MISSING_WRITER -> datumBytes;
            case JSON_ENCODING -> {
                headers.add(KafkaSerdeHeaders.HEADER_VALUE_ENCODING, "JSON".getBytes(StandardCharsets.UTF_8));
                yield DeadLetterTestRecords.prefix(this.currentCoordinates, datumBytes);
            }
            default -> DeadLetterTestRecords.prefix(this.currentCoordinates, datumBytes);
        };
        RecordMetadata rejectedRecord = this.send(0, DUPLICATE_TIME, eventId, valueBytes, headers);
        this.sendCurrent(followerEventId, 0, 0, DUPLICATE_TIME);
        Map<TopicPartition, Long> endOffsets = this.applicationAndFixtureEndOffsets();
        Position rejectedPosition = Position.of(rejectedRecord);
        this.timestampOverride = switch (invalidRecord) {
            case MISSING_TIMESTAMP -> new TimestampOverride(rejectedPosition, null);
            case NEGATIVE_TIMESTAMP -> new TimestampOverride(rejectedPosition, -1L);
            default -> null;
        };
        this.startConsumer();
        for (int delivery = 0; delivery < 2; delivery++) {
            assertFailure(this.nextConsumption(), rejectedPosition, invalidRecord.exceptionType, invalidRecord.reason);
        }
        this.stopConsumer();

        this.assertCommittedOffset(rejectedRecord, priorCommittedOffset);
        Assertions.assertThat(this.generations(followerEventId)).isEmpty();
        if (Objects.isNull(priorOccurrence)) {
            Assertions.assertThat(this.generations(eventId)).isEmpty();
        } else {
            Assertions.assertThat(this.generations(eventId)).containsExactly(0);
            Assertions.assertThat(this.occurrence(eventId, 0)).isEqualTo(priorOccurrence);
        }
        Assertions.assertThat(this.applicationAndFixtureEndOffsets()).isEqualTo(endOffsets);
    }

    private Map<TopicPartition, Long> applicationAndFixtureEndOffsets() throws Exception {
        String[] topics = Stream.concat(DeadLetterTestRecords.APPLICATION_TOPICS.stream(), Stream.of(this.topicName)).toArray(String[]::new);
        return KafkaTestRecords.endOffsets(this.adminClient, topics);
    }

    private void startConsumer() throws Exception {
        this.camelContext.start();
        this.camelContext.getRouteController().startRoute(DeadLetterTestRecords.ROUTE_ID);
    }

    private void awaitIndependentPartitionAssignments() throws InterruptedException {
        Set<TopicPartition> expectedPartitions = Set.of(new TopicPartition(this.topicName, 0), new TopicPartition(this.topicName, 1), new TopicPartition(this.topicName, 2));
        Instant deadline = Instant.now().plusSeconds(20);
        while (Instant.now().isBefore(deadline)) {
            try {
                ConsumerGroupDescription consumerGroupDescription = this.adminClient
                        .describeConsumerGroups(List.of(DeadLetterTestRecords.GROUP_ID))
                        .describedGroups()
                        .get(DeadLetterTestRecords.GROUP_ID)
                        .get(2, TimeUnit.SECONDS);
                if (Objects.equals(consumerGroupDescription.groupState(), GroupState.STABLE)) {
                    Set<TopicPartition> independentlyAssignedPartitions = consumerGroupDescription
                            .members()
                            .stream()
                            .map(memberDescription -> memberDescription.assignment().topicPartitions())
                            .filter(topicPartitions -> topicPartitions.size() == 1)
                            .flatMap(Set::stream)
                            .collect(Collectors.toSet());
                    if (Objects.equals(independentlyAssignedPartitions, expectedPartitions)) {
                        return;
                    }
                }
            } catch (ExecutionException | TimeoutException exception) {
                // The group can be absent or rebalancing while the polling threads start.
            }
            Thread.sleep(50L);
        }
        throw new AssertionError("Fixture partitions were not assigned to separate consumers: " + expectedPartitions);
    }

    private static void awaitKafkaPause(org.apache.camel.component.kafka.KafkaConsumer kafkaConsumer) throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(20);
        while (!kafkaConsumer.isKafkaPaused() && Instant.now().isBefore(deadline)) {
            Thread.sleep(50L);
        }
        Assertions.assertThat(kafkaConsumer.isKafkaPaused()).as("storage consumer paused").isTrue();
    }

    private void stopConsumer() throws Exception {
        RouteController routeController = this.camelContext.getRouteController();
        ServiceStatus serviceStatus = routeController.getRouteStatus(DeadLetterTestRecords.ROUTE_ID);
        if (serviceStatus.isStarted() || serviceStatus.isSuspended()) {
            Assertions.assertThat(routeController.stopRoute(DeadLetterTestRecords.ROUTE_ID, 10, TimeUnit.SECONDS, false)).isTrue();
        }
    }

    private RecordMetadata sendCurrent(UUID eventId, int replayCount, int partition, Instant recordTime) throws Exception {
        GenericRecord genericRecord = DeadLetterTestRecords.terminalDatum(eventId.toString(), replayCount, null);
        return this.send(partition, recordTime, eventId, this.currentValue(genericRecord), new RecordHeaders());
    }

    private byte[] currentValue(GenericRecord genericRecord) throws IOException {
        return DeadLetterTestRecords.prefix(this.currentCoordinates, WebhookEnvelopeFixtures.writeDatum(genericRecord));
    }

    private RecordMetadata send(int partition, Instant recordTime, UUID eventId, byte[] valueBytes, Headers headers) throws Exception {
        return DeadLetterTestRecords.send(this.kafkaProducer, new TopicPartition(this.topicName, partition), recordTime, eventId.toString(), valueBytes, headers);
    }

    private static Headers signedHeaders() {
        return new RecordHeaders().add("signing-secret", SIGNING_MARKER.getBytes(StandardCharsets.UTF_8));
    }

    private void assertCommittedOffset(RecordMetadata recordMetadata, long expectedOffset) throws Exception {
        Position position = Position.of(recordMetadata);
        Assertions.assertThat(this.committedOffset(position)).as("committed offset for %s", position.topicPartition()).isEqualTo(expectedOffset);
    }

    private void awaitCommit(RecordMetadata recordMetadata) throws Exception {
        Position position = Position.of(recordMetadata);
        long expectedOffset = position.offset() + 1;
        Instant deadline = Instant.now().plusSeconds(20);
        while (Instant.now().isBefore(deadline)) {
            long committedOffset = this.committedOffset(position);
            if (committedOffset >= expectedOffset) {
                Assertions.assertThat(committedOffset).as("next offset for %s", position.topicPartition()).isEqualTo(expectedOffset);
                return;
            }
            Thread.sleep(50L);
        }
        throw new AssertionError("Offset was not committed for " + position);
    }

    private long committedOffset(Position position) throws Exception {
        return KafkaTestRecords.committedOffset(this.adminClient, DeadLetterTestRecords.GROUP_ID, position.topicPartition());
    }

    private long earliestOffset(Position position) throws Exception {
        TopicPartition topicPartition = position.topicPartition();
        return this.adminClient.listOffsets(Map.of(topicPartition, OffsetSpec.earliest())).partitionResult(topicPartition).get(5, TimeUnit.SECONDS).offset();
    }

    private List<Integer> generations(UUID eventId) {
        return this.jdbcTemplate.queryForList("SELECT replay_count FROM dead_letter_occurrence WHERE event_id = ? ORDER BY replay_count", Integer.class, eventId);
    }

    private OccurrenceSnapshot acceptReplay(UUID eventId, int replayCount) {
        OccurrenceSnapshot occurrenceSnapshot = this.occurrence(eventId, replayCount);
        Assertions.assertThat(occurrenceSnapshot.replayAccepted()).isFalse();
        Assertions.assertThat(this.jdbcTemplate.update("UPDATE dead_letter_occurrence SET replay_accepted = true WHERE occurrence_id = ?", occurrenceSnapshot.occurrenceId())).isEqualTo(1);
        OccurrenceSnapshot acceptedOccurrence = this.occurrence(eventId, replayCount);
        Assertions.assertThat(acceptedOccurrence.replayAccepted()).isTrue();
        return acceptedOccurrence;
    }

    private OccurrenceSnapshot occurrence(UUID eventId, int replayCount) {
        DeadLetterOccurrence deadLetterOccurrence = this.deadLetterOccurrenceRepository.findByEventIdAndReplayCount(eventId, replayCount).orElseThrow();
        return new OccurrenceSnapshot(
                deadLetterOccurrence.getOccurrenceId(),
                deadLetterOccurrence.getEventId(),
                deadLetterOccurrence.getReplayCount(),
                deadLetterOccurrence.getDeadLetteredAt(),
                deadLetterOccurrence.isReplayAccepted(),
                deadLetterOccurrence.getStoredWebhookEnvelope()
        );
    }

    private void observeConsumption(Exchange exchange) throws InterruptedException {
        Message message = exchange.getMessage();
        Position position = new Position(
                new TopicPartition(message.getHeader(KafkaConstants.TOPIC, String.class), message.getHeader(KafkaConstants.PARTITION, Integer.class)),
                message.getHeader(KafkaConstants.OFFSET, Long.class)
        );
        exchange.getUnitOfWork().addSynchronization(new Synchronization() {
            @Override
            public void onComplete(@NonNull Exchange exchange) {
                DeadLetterConsumerIntegrationTests.this.consumptions.add(new Consumption(position, null));
            }

            @Override
            public void onFailure(@NonNull Exchange exchange) {
                DeadLetterConsumerIntegrationTests.this.consumptions.add(new Consumption(position, exchange.getException()));
            }
        });

        TimestampOverride timestampOverride = this.timestampOverride;
        if (Objects.nonNull(timestampOverride) && Objects.equals(timestampOverride.position(), position)) {
            // The Kafka producer rejects negative timestamps and assigns a timestamp when it is omitted.
            if (Objects.isNull(timestampOverride.timestampMillis())) {
                message.removeHeader(KafkaConstants.TIMESTAMP);
            } else {
                message.setHeader(KafkaConstants.TIMESTAMP, timestampOverride.timestampMillis());
            }
        }
        if (Objects.equals(this.heldFollower, position)) {
            this.followerReached.countDown();
            if (!this.releaseFollower.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Fixture did not release the following record");
            }
        }
    }

    private Consumption nextConsumption() throws InterruptedException {
        Consumption consumption = this.consumptions.poll(20, TimeUnit.SECONDS);
        Assertions.assertThat(consumption).as("dead-letter consumer outcome").isNotNull();
        return consumption;
    }

    private static void assertFailure(Consumption consumption, Position position, Class<? extends Throwable> exceptionType, String expectedMessage) {
        Assertions.assertThat(consumption.position()).isEqualTo(position);
        Assertions.assertThat(consumption.failure()).isInstanceOf(exceptionType).hasMessage(expectedMessage).hasNoCause();
        Assertions.assertThat(consumption.failure().getSuppressed()).isEmpty();
    }

    private enum Identifier {
        PREFIX, CONTENT_ID, CONTENT_HASH, GLOBAL_ID
    }

    @RequiredArgsConstructor
    private enum InvalidRecord {
        TOMBSTONE(InvalidWebhookEnvelopeException.class, MALFORMED_FRAMING),
        MALFORMED_PREFIX(InvalidWebhookEnvelopeException.class, MALFORMED_FRAMING),
        MALFORMED_DUPLICATE(InvalidWebhookEnvelopeException.class, "Webhook envelope datum is malformed"),
        INCOMPATIBLE_WRITER(InvalidWebhookEnvelopeException.class, "Webhook envelope writer schema is incompatible"),
        UNKNOWN_WRITER(InvalidWebhookEnvelopeException.class, "Webhook envelope writer schema could not be resolved"),
        MISSING_WRITER(InvalidWebhookEnvelopeException.class, "Webhook envelope writer schema is missing"),
        JSON_ENCODING(InvalidWebhookEnvelopeException.class, "Apicurio webhook envelope must use binary Avro"),
        NONCANONICAL_UUID(IllegalArgumentException.class, INVALID_ID),
        INVALID_UUID(IllegalArgumentException.class, INVALID_ID),
        NEGATIVE_REPLAY(IllegalArgumentException.class, "Dead-letter replay count must be nonnegative"),
        MISSING_TIMESTAMP(IllegalArgumentException.class, INVALID_TIMESTAMP),
        NEGATIVE_TIMESTAMP(IllegalArgumentException.class, INVALID_TIMESTAMP);

        private final Class<? extends Throwable> exceptionType;
        private final String reason;
    }

    private record Position(TopicPartition topicPartition, long offset) {

        static Position of(RecordMetadata recordMetadata) {
            return new Position(new TopicPartition(recordMetadata.topic(), recordMetadata.partition()), recordMetadata.offset());
        }
    }

    private record Consumption(Position position, Throwable failure) {
    }

    private record TimestampOverride(Position position, Long timestampMillis) {
    }

    private record OccurrenceSnapshot(UUID occurrenceId, UUID eventId, int replayCount, Instant deadLetteredAt, boolean replayAccepted, StoredWebhookEnvelope storedWebhookEnvelope) {
    }
}
