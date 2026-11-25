package gregor.integration;

import gregor.avro.ApicurioWebhookEnvelopeReader;
import gregor.avro.WebhookEnvelopeCodec;
import gregor.avro.WebhookEnvelopeFixtures;
import gregor.dto.StoredWebhookEnvelope;
import gregor.entity.DeadLetterOccurrence;
import gregor.exception.InvalidWebhookEnvelopeException;
import gregor.repository.DeadLetterOccurrenceRepository;
import gregor.service.DeadLetterOccurrenceService;
import io.apicurio.registry.resolver.client.RegistryArtifactReference;
import io.apicurio.registry.resolver.client.RegistryClientFacade;
import io.apicurio.registry.resolver.client.RegistryClientFacadeFactory;
import io.apicurio.registry.resolver.client.RegistryVersionCoordinates;
import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.apicurio.registry.resolver.strategy.ArtifactReference;
import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.client.admin.config.properties.item.WithPropertyNameItemRequestBuilder;
import io.apicurio.registry.rest.client.models.ProblemDetails;
import io.apicurio.registry.rest.client.models.UpdateConfigurationProperty;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import io.apicurio.registry.serde.avro.AvroSerdeConfig;
import io.apicurio.registry.serde.avro.AvroSerdeHeaders;
import io.apicurio.registry.serde.config.SerdeConfig;
import io.apicurio.registry.serde.fallback.FallbackArtifactProvider;
import io.apicurio.registry.serde.kafka.config.KafkaSerdeConfig;
import io.apicurio.registry.serde.kafka.headers.KafkaSerdeHeaders;
import io.vertx.core.Vertx;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SequencedSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.datasource.url=jdbc:postgresql://${POSTGRES_HOST:localhost}:${POSTGRES_PORT:5432}/${POSTGRES_DB:gregor}",
                "spring.datasource.username=${POSTGRES_USER:gregor}",
                "spring.datasource.password=${POSTGRES_PASSWORD:gregor}",
                "spring.jpa.hibernate.ddl-auto=update"
        }
)
class ApicurioWebhookEnvelopeReaderIntegrationTests {

    private static final String ARTIFACT_DELETION_PROPERTY = "apicurio.rest.deletion.artifact.enabled";
    private static final String MALFORMED_FRAMING = "Apicurio webhook envelope framing is malformed";
    private static final String NONBINARY_ENVELOPE = "Apicurio webhook envelope must use binary Avro";
    private static final String MISSING_SCHEMA = "Webhook envelope writer schema is missing";
    private static final String UNRESOLVABLE_SCHEMA = "Webhook envelope writer schema could not be resolved";

    private final SequencedSet<String> fixtureArtifactIds = new LinkedHashSet<>();

    @Autowired
    private Environment environment;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private Vertx registryVertx;
    @Autowired
    private ApicurioWebhookEnvelopeReader apicurioWebhookEnvelopeReader;
    @Autowired
    private WebhookEnvelopeCodec webhookEnvelopeCodec;
    @Autowired
    private DeadLetterOccurrenceService deadLetterOccurrenceService;
    @Autowired
    private DeadLetterOccurrenceRepository deadLetterOccurrenceRepository;

    @Test
    void readsRegisteredPhaseOneWriterWithDefaultPrefixFraming() throws Exception {
        Schema writerSchema = WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA;
        GenericRecord genericRecord = WebhookEnvelopeFixtures.phaseOneEvent(writerSchema, WebhookEnvelopeFixtures.EVENT_ID);
        Map<String, Object> serdeProperties = this.serdeProperties();
        String topicName = this.fixtureTopicName();
        Headers headers = new RecordHeaders();
        byte[] valueBytes = serialize(serdeProperties, topicName, headers, genericRecord);

        Assertions.assertThat(headers).isEmpty();
        StoredWebhookEnvelope storedWebhookEnvelope = this.apicurioWebhookEnvelopeReader.read(topicName, headers, valueBytes);
        Assertions.assertThat(new Schema.Parser().parse(storedWebhookEnvelope.writerSchemaJson())).isEqualTo(writerSchema);
        Assertions.assertThat(storedWebhookEnvelope.datumBytes()).containsExactly(WebhookEnvelopeFixtures.writeDatum(genericRecord));
        WebhookEnvelopeFixtures.assertPhaseOneEvent(this.webhookEnvelopeCodec.decode(storedWebhookEnvelope), WebhookEnvelopeFixtures.EVENT_ID);

        byte[] trailingValueBytes = Arrays.copyOf(valueBytes, valueBytes.length + 1);
        assertFailure(() -> this.apicurioWebhookEnvelopeReader.read(topicName, headers, trailingValueBytes), "Webhook envelope datum is malformed");
    }

    @Test
    void persistsTheOriginalWriterAndItsNamedReferenceInsteadOfTheNewestRegistryVersions() throws Exception {
        String topicName = this.fixtureTopicName();
        String referenceName = "ArchivedContext_" + UUID.randomUUID().toString().replace("-", "");
        Schema writerSchema = WebhookEnvelopeFixtures.phaseOneSchemaWith(
                """
                {"name":"writerContext","type":{"type":"record","name":"%s","fields":[{"name":"reason","type":"string"}]}}
                """.formatted(referenceName)
        );
        Schema referenceSchema = writerSchema.getField("writerContext").schema();
        // Apicurio registers named references under their full Avro names. Delete the writer first.
        this.fixtureArtifactIds.add(referenceSchema.getFullName());
        UUID eventId = UUID.randomUUID();
        GenericRecord genericRecord = WebhookEnvelopeFixtures.phaseOneEvent(writerSchema, eventId.toString());
        GenericRecord contextGenericRecord = new GenericData.Record(referenceSchema);
        contextGenericRecord.put("reason", "legacy \uD83D\uDE80 { }");
        genericRecord.put("writerContext", contextGenericRecord);
        Map<String, Object> serdeProperties = this.serdeProperties();
        RegistryClientFacade registryClientFacade = RegistryClientFacadeFactory.create(new SchemaResolverConfig(serdeProperties));
        Headers headers = new RecordHeaders();
        byte[] valueBytes = serialize(serdeProperties, topicName, headers, genericRecord);
        RegistryVersionCoordinates originalCoordinates = registryClientFacade.getVersionCoordinatesByGAV("default", topicName + "-value", null);
        Set<RegistryArtifactReference> registryArtifactReferences = Set.copyOf(registryClientFacade.getReferencesByGlobalId(originalCoordinates.getGlobalId()));
        Assertions.assertThat(registryArtifactReferences).hasSize(1);
        RegistryArtifactReference registryArtifactReference = registryArtifactReferences.iterator().next();
        String referenceGroupId = Objects.requireNonNullElse(registryArtifactReference.getGroupId(), "default");
        RegistryVersionCoordinates latestReferenceCoordinates = registryClientFacade.createSchema(
                "AVRO",
                referenceGroupId,
                registryArtifactReference.getArtifactId(),
                null,
                "CREATE_VERSION",
                false,
                """
                {"type":"record","name":"%s","namespace":"gregor.avro","fields":[{"name":"reason","type":"string"},{"name":"later","type":"int","default":0}]}
                """.formatted(referenceName),
                Set.of()
        );
        Assertions.assertThat(latestReferenceCoordinates.getVersion()).isNotEqualTo(registryArtifactReference.getVersion());
        serialize(serdeProperties, topicName, new RecordHeaders(), WebhookEnvelopeFixtures.phaseOneEvent(WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA, eventId.toString()));
        RegistryVersionCoordinates latestCoordinates = registryClientFacade.getVersionCoordinatesByGAV("default", topicName + "-value", null);
        Assertions.assertThat(latestCoordinates.getGlobalId()).isGreaterThan(originalCoordinates.getGlobalId());

        StoredWebhookEnvelope storedWebhookEnvelope = this.apicurioWebhookEnvelopeReader.read(topicName, headers, valueBytes);
        UUID occurrenceId = UUID.randomUUID();
        this.deadLetterOccurrenceService.insert(occurrenceId, storedWebhookEnvelope, Instant.now().truncatedTo(ChronoUnit.MILLIS));
        DeadLetterOccurrence deadLetterOccurrence = this.deadLetterOccurrenceRepository.findById(occurrenceId).orElseThrow();
        StoredWebhookEnvelope reloadedStoredWebhookEnvelope = deadLetterOccurrence.getStoredWebhookEnvelope();

        // Remove the writer artifact and its named reference before reconstructing from PostgreSQL.
        this.removeFixtureArtifacts();
        WebhookEnvelopeFixtures.assertPhaseOneEvent(new WebhookEnvelopeCodec().decode(reloadedStoredWebhookEnvelope), eventId.toString());
        Schema retainedSchema = new Schema.Parser().parse(reloadedStoredWebhookEnvelope.writerSchemaJson());
        Assertions.assertThat(retainedSchema).isEqualTo(writerSchema);
        Assertions.assertThat(reloadedStoredWebhookEnvelope.datumBytes()).containsExactly(WebhookEnvelopeFixtures.writeDatum(genericRecord));
        BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(reloadedStoredWebhookEnvelope.datumBytes(), null);
        GenericRecord retainedGenericRecord = new GenericDatumReader<GenericRecord>(retainedSchema).read(null, binaryDecoder);
        GenericRecord retainedContextGenericRecord = (GenericRecord) retainedGenericRecord.get("writerContext");
        Assertions.assertThat(retainedContextGenericRecord.get("reason").toString()).isEqualTo("legacy \uD83D\uDE80 { }");
        Assertions.assertThat(binaryDecoder.isEnd()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"same version", "reordered properties", "conflicting version"})
    void resolvesSharedTransitiveDefinitionsOnlyWhenTheyAgree(String referenceVariant) throws Exception {
        Map<String, Object> serdeProperties = this.headerSerdeProperties();
        RegistryClientFacade registryClientFacade = RegistryClientFacadeFactory.create(new SchemaResolverConfig(serdeProperties));
        String topicName = this.fixtureTopicName();
        String sharedSchemaJson = """
                                  {"type":"record","name":"gregor.avro.SharedContext","fields":[{"name":"archived","type":"boolean"}],"source":"legacy","detail":{"first":1,"second":2}}
                                  """;
        String rightSharedSchemaJson = switch (referenceVariant) {
            case "same version" -> sharedSchemaJson;
            case "reordered properties" -> sharedSchemaJson.replace("\"source\":\"legacy\",\"detail\":{\"first\":1,\"second\":2}", "\"detail\":{\"second\":2,\"first\":1},\"source\":\"legacy\"");
            case "conflicting version" -> sharedSchemaJson.replace("archived", "retained");
            default -> throw new IllegalArgumentException(referenceVariant);
        };
        RegistryArtifactReference sharedRegistryArtifactReference = this.registerReference(registryClientFacade, topicName + "-shared", "gregor.avro.SharedContext", sharedSchemaJson, Set.of());
        RegistryArtifactReference rightSharedRegistryArtifactReference = Objects.equals("same version", referenceVariant)
                ? sharedRegistryArtifactReference
                : this.registerReference(registryClientFacade, topicName + "-shared", "gregor.avro.SharedContext", rightSharedSchemaJson, Set.of());
        if (!Objects.equals("same version", referenceVariant)) {
            Assertions.assertThat(rightSharedRegistryArtifactReference.getVersion()).isNotEqualTo(sharedRegistryArtifactReference.getVersion());
            Assertions.assertThat(registryClientFacade.getSchemaByGAV("default", topicName + "-shared", rightSharedRegistryArtifactReference.getVersion())).isEqualTo(rightSharedSchemaJson);
        }
        String branchSchemaJson = """
                                  {"type":"record","name":"gregor.avro.%s","fields":[{"name":"context","type":"SharedContext"}]}
                                  """;
        RegistryArtifactReference leftRegistryArtifactReference = this.registerReference(
                registryClientFacade,
                topicName + "-left",
                "gregor.avro.LeftContext",
                branchSchemaJson.formatted("LeftContext"),
                Set.of(sharedRegistryArtifactReference)
        );
        RegistryArtifactReference rightRegistryArtifactReference = this.registerReference(
                registryClientFacade,
                topicName + "-right",
                "gregor.avro.RightContext",
                branchSchemaJson.formatted("RightContext"),
                Set.of(rightSharedRegistryArtifactReference)
        );
        RegistryVersionCoordinates coordinates = this.registerSchema(
                registryClientFacade,
                topicName + "-value",
                WebhookEnvelopeFixtures.phaseOneSchemaJsonWith(
                        """
                        {"name":"left","type":"LeftContext"}, {"name":"right","type":"RightContext"}
                        """
                ),
                Set.of(leftRegistryArtifactReference, rightRegistryArtifactReference)
        );
        // One boolean in each branch. Both versions have the same binary layout.
        byte[] datumBytes = WebhookEnvelopeFixtures.phaseOneDatumWith((byte) 1, (byte) 0);
        Headers headers = globalIdHeaders(coordinates);

        try (ApicurioWebhookEnvelopeReader apicurioWebhookEnvelopeReader = new ApicurioWebhookEnvelopeReader(this.webhookEnvelopeCodec, serdeProperties)) {
            if (Objects.equals("conflicting version", referenceVariant)) {
                assertFailure(() -> apicurioWebhookEnvelopeReader.read(topicName, headers, datumBytes), UNRESOLVABLE_SCHEMA);
            } else {
                StoredWebhookEnvelope storedWebhookEnvelope = apicurioWebhookEnvelopeReader.read(topicName, headers, datumBytes);
                Assertions.assertThat(storedWebhookEnvelope.datumBytes()).containsExactly(datumBytes);
                Schema retainedSchema = new Schema.Parser().parse(storedWebhookEnvelope.writerSchemaJson());
                Schema expectedSharedSchema = new Schema.Parser().parse(sharedSchemaJson);
                Assertions.assertThat(retainedSchema.getField("left").schema().getField("context").schema()).isEqualTo(expectedSharedSchema);
                Assertions.assertThat(retainedSchema.getField("right").schema().getField("context").schema()).isEqualTo(expectedSharedSchema);
                WebhookEnvelopeFixtures.assertPhaseOneEvent(this.webhookEnvelopeCodec.decode(storedWebhookEnvelope), WebhookEnvelopeFixtures.EVENT_ID);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"\"doc\":\"Inline context\",", "\"aliases\":[\"ArchivedContext\"],", ""})
    void checksInlineDefinitionsEvenWhenOtherReferencesAreRequired(String inlineMetadataJson) throws Exception {
        Map<String, Object> serdeProperties = this.headerSerdeProperties();
        RegistryClientFacade registryClientFacade = RegistryClientFacadeFactory.create(new SchemaResolverConfig(serdeProperties));
        String topicName = this.fixtureTopicName();
        String sharedSchemaJson = """
                                  {"type":"record","name":"gregor.avro.SharedContext",%s"fields":[{"name":"archived","type":"boolean"}]}
                                  """;
        RegistryArtifactReference sharedRegistryArtifactReference = this.registerReference(
                registryClientFacade,
                topicName + "-shared",
                "gregor.avro.SharedContext",
                sharedSchemaJson.formatted(""),
                Set.of()
        );
        RegistryArtifactReference externalRegistryArtifactReference = this.registerReference(
                registryClientFacade,
                topicName + "-external",
                "gregor.avro.ExternalContext",
                """
                {"type":"record","name":"gregor.avro.ExternalContext","fields":[{"name":"verified","type":"boolean"}]}
                """,
                Set.of()
        );
        RegistryVersionCoordinates coordinates = this.registerSchema(
                registryClientFacade,
                topicName + "-value",
                WebhookEnvelopeFixtures.phaseOneSchemaJsonWith(
                        """
                        {"name":"inlineContext","type":%s}, {"name":"externalContext","type":"ExternalContext"}
                        """.formatted(sharedSchemaJson.formatted(inlineMetadataJson))
                ),
                Set.of(sharedRegistryArtifactReference, externalRegistryArtifactReference)
        );
        Headers headers = globalIdHeaders(coordinates);
        byte[] datumBytes = WebhookEnvelopeFixtures.phaseOneDatumWith((byte) 1, (byte) 0);

        try (ApicurioWebhookEnvelopeReader apicurioWebhookEnvelopeReader = new ApicurioWebhookEnvelopeReader(this.webhookEnvelopeCodec, serdeProperties)) {
            if (StringUtils.isNotEmpty(inlineMetadataJson)) {
                assertFailure(() -> apicurioWebhookEnvelopeReader.read(topicName, headers, datumBytes), UNRESOLVABLE_SCHEMA);
            } else {
                StoredWebhookEnvelope storedWebhookEnvelope = apicurioWebhookEnvelopeReader.read(topicName, headers, datumBytes);
                Assertions.assertThat(storedWebhookEnvelope.datumBytes()).containsExactly(datumBytes);
                Schema retainedSchema = new Schema.Parser().parse(storedWebhookEnvelope.writerSchemaJson());
                Assertions.assertThat(retainedSchema.getField("inlineContext").schema().getField("archived").schema().getType()).isEqualTo(Schema.Type.BOOLEAN);
                Assertions.assertThat(retainedSchema.getField("externalContext").schema().getField("verified").schema().getType()).isEqualTo(Schema.Type.BOOLEAN);
                WebhookEnvelopeFixtures.assertPhaseOneEvent(this.webhookEnvelopeCodec.decode(storedWebhookEnvelope), WebhookEnvelopeFixtures.EVENT_ID);
            }
        }
    }

    @Test
    void resolvesRegisteredNameInWriterNamespaceBeforeAnInlineEmptyNamespaceName() throws Exception {
        Map<String, Object> serdeProperties = this.headerSerdeProperties();
        RegistryClientFacade registryClientFacade = RegistryClientFacadeFactory.create(new SchemaResolverConfig(serdeProperties));
        String topicName = this.fixtureTopicName();
        RegistryArtifactReference registryArtifactReference = this.registerReference(
                registryClientFacade,
                topicName + "-reference",
                "gregor.avro.ArchivedContext",
                """
                {"type":"record","name":"gregor.avro.ArchivedContext","fields":[{"name":"archived","type":"boolean"}]}
                """,
                Set.of()
        );
        RegistryVersionCoordinates coordinates = this.registerSchema(
                registryClientFacade,
                topicName + "-value",
                WebhookEnvelopeFixtures.phaseOneSchemaJsonWith(
                        """
                        {"name":"inlineContext","type":{"type":"record","name":"ArchivedContext","namespace":"","fields":[{"name":"inlineFlag","type":"boolean"}]}},
                        {"name":"registeredContext","type":"ArchivedContext"}
                        """
                ),
                Set.of(registryArtifactReference)
        );
        byte[] datumBytes = WebhookEnvelopeFixtures.phaseOneDatumWith((byte) 1, (byte) 0);

        try (ApicurioWebhookEnvelopeReader apicurioWebhookEnvelopeReader = new ApicurioWebhookEnvelopeReader(this.webhookEnvelopeCodec, serdeProperties)) {
            StoredWebhookEnvelope storedWebhookEnvelope = apicurioWebhookEnvelopeReader.read(topicName, globalIdHeaders(coordinates), datumBytes);
            Assertions.assertThat(storedWebhookEnvelope.datumBytes()).containsExactly(datumBytes);
            Schema retainedSchema = new Schema.Parser().parse(storedWebhookEnvelope.writerSchemaJson());
            BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(storedWebhookEnvelope.datumBytes(), null);
            GenericRecord retainedGenericRecord = new GenericDatumReader<GenericRecord>(retainedSchema).read(null, binaryDecoder);
            GenericRecord inlineGenericRecord = (GenericRecord) retainedGenericRecord.get("inlineContext");
            GenericRecord registeredGenericRecord = (GenericRecord) retainedGenericRecord.get("registeredContext");
            Assertions.assertThat(inlineGenericRecord.getSchema().getFullName()).isEqualTo("ArchivedContext");
            Assertions.assertThat(inlineGenericRecord.get("inlineFlag")).isEqualTo(true);
            Assertions.assertThat(registeredGenericRecord.getSchema().getFullName()).isEqualTo("gregor.avro.ArchivedContext");
            Assertions.assertThat(registeredGenericRecord.get("archived")).isEqualTo(false);
            WebhookEnvelopeFixtures.assertPhaseOneEvent(this.webhookEnvelopeCodec.decode(storedWebhookEnvelope), WebhookEnvelopeFixtures.EVENT_ID);
        }
    }

    @Test
    void retainsEmptyNamespaceReferenceWhenAnInlineNameShadowsIt() throws Exception {
        Map<String, Object> serdeProperties = this.headerSerdeProperties();
        RegistryClientFacade registryClientFacade = RegistryClientFacadeFactory.create(new SchemaResolverConfig(serdeProperties));
        String topicName = this.fixtureTopicName();
        RegistryArtifactReference archivedRegistryArtifactReference = this.registerReference(
                registryClientFacade,
                topicName + "-archive",
                "ArchivedContext",
                """
                {"type":"record","name":"ArchivedContext","fields":[{"name":"archived","type":"boolean"}]}
                """,
                Set.of()
        );
        RegistryArtifactReference wrapperRegistryArtifactReference = this.registerReference(
                registryClientFacade,
                topicName + "-wrapper",
                "gregor.avro.ContextWrapper",
                """
                {"type":"record","name":"gregor.avro.ContextWrapper","fields":[{"name":"context","type":"ArchivedContext"}]}
                """,
                Set.of(archivedRegistryArtifactReference)
        );
        RegistryVersionCoordinates coordinates = this.registerSchema(
                registryClientFacade,
                topicName + "-value",
                WebhookEnvelopeFixtures.phaseOneSchemaJsonWith(
                        """
                        {"name":"firstContext","type":"ArchivedContext"},
                        {"name":"inlineContext","type":{"type":"record","name":"ArchivedContext","fields":[{"name":"inlineFlag","type":"boolean"}]}},
                        {"name":"wrapper","type":"ContextWrapper"}
                        """
                ),
                Set.of(archivedRegistryArtifactReference, wrapperRegistryArtifactReference)
        );
        byte[] datumBytes = WebhookEnvelopeFixtures.phaseOneDatumWith((byte) 1, (byte) 0, (byte) 1);
        StoredWebhookEnvelope storedWebhookEnvelope;
        try (ApicurioWebhookEnvelopeReader apicurioWebhookEnvelopeReader = new ApicurioWebhookEnvelopeReader(this.webhookEnvelopeCodec, serdeProperties)) {
            storedWebhookEnvelope = apicurioWebhookEnvelopeReader.read(topicName, globalIdHeaders(coordinates), datumBytes);
        }

        this.removeFixtureArtifacts();
        WebhookEnvelopeFixtures.assertPhaseOneEvent(this.webhookEnvelopeCodec.decode(storedWebhookEnvelope), WebhookEnvelopeFixtures.EVENT_ID);
        Assertions.assertThat(storedWebhookEnvelope.datumBytes()).containsExactly(datumBytes);
        Schema retainedSchema = WebhookEnvelopeFixtures.parseStoredWriterSchema(storedWebhookEnvelope);
        BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(storedWebhookEnvelope.datumBytes(), null);
        GenericRecord retainedGenericRecord = new GenericDatumReader<GenericRecord>(retainedSchema).read(null, binaryDecoder);
        GenericRecord firstGenericRecord = (GenericRecord) retainedGenericRecord.get("firstContext");
        GenericRecord inlineGenericRecord = (GenericRecord) retainedGenericRecord.get("inlineContext");
        GenericRecord wrapperGenericRecord = (GenericRecord) retainedGenericRecord.get("wrapper");
        GenericRecord nestedGenericRecord = (GenericRecord) wrapperGenericRecord.get("context");
        Assertions.assertThat(firstGenericRecord.getSchema().getFullName()).isEqualTo("ArchivedContext");
        Assertions.assertThat(firstGenericRecord.get("archived")).isEqualTo(true);
        Assertions.assertThat(inlineGenericRecord.getSchema().getFullName()).isEqualTo("gregor.avro.ArchivedContext");
        Assertions.assertThat(inlineGenericRecord.get("inlineFlag")).isEqualTo(false);
        Assertions.assertThat(nestedGenericRecord.getSchema().getFullName()).isEqualTo("ArchivedContext");
        Assertions.assertThat(nestedGenericRecord.get("archived")).isEqualTo(true);
        Assertions.assertThat(binaryDecoder.isEnd()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"\"doc\":\"Different detail\",", "\"aliases\":[\"LegacyDetail\"],"})
    void rejectsConflictingNamedDefinitionsInAnUnusedRegistryReference(String conflictingMetadataJson) throws Exception {
        Map<String, Object> serdeProperties = this.headerSerdeProperties();
        RegistryClientFacade registryClientFacade = RegistryClientFacadeFactory.create(new SchemaResolverConfig(serdeProperties));
        String topicName = this.fixtureTopicName();
        RegistryArtifactReference registryArtifactReference = this.registerReference(
                registryClientFacade,
                topicName + "-unused",
                "gregor.avro.UnusedContext",
                """
                {"type":"record","name":"gregor.avro.UnusedContext","fields":[
                    {"name":"firstDetail","type":{"type":"record","name":"Detail","fields":[{"name":"verified","type":"boolean"}]}},
                    {"name":"secondDetail","type":{"type":"record","name":"Detail",%s"fields":[{"name":"verified","type":"boolean"}]}}
                ]}
                """.formatted(conflictingMetadataJson),
                Set.of()
        );
        RegistryVersionCoordinates coordinates = this.registerSchema(
                registryClientFacade,
                topicName + "-value",
                WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA.toString(),
                Set.of(registryArtifactReference)
        );
        Headers headers = globalIdHeaders(coordinates);
        byte[] datumBytes = WebhookEnvelopeFixtures.phaseOneDatumWith();

        try (ApicurioWebhookEnvelopeReader apicurioWebhookEnvelopeReader = new ApicurioWebhookEnvelopeReader(this.webhookEnvelopeCodec, serdeProperties)) {
            assertFailure(() -> apicurioWebhookEnvelopeReader.read(topicName, headers, datumBytes), UNRESOLVABLE_SCHEMA);
        }
    }

    @ParameterizedTest
    @CsvSource({"false, true", "false, false", "true, true", "true, false"})
    void rejectsNonJsonRegistrySchemaTextBeforeAvroParsing(boolean malformedReference, boolean containsComment) throws Exception {
        Map<String, Object> serdeProperties = this.headerSerdeProperties();
        RegistryClientFacade registryClientFacade = RegistryClientFacadeFactory.create(new SchemaResolverConfig(serdeProperties));
        String topicName = this.fixtureTopicName();
        String writerSchemaJson = WebhookEnvelopeFixtures.phaseOneSchemaJsonWith(
                """
                {"name":"context","type":"ArchivedContext"}
                """
        );
        String referenceSchemaJson = """
                                     {"type":"record","name":"gregor.avro.ArchivedContext","fields":[{"name":"archived","type":"boolean"}]}
                                     """;
        String malformedSchemaJson = malformedReference ? referenceSchemaJson : writerSchemaJson;
        malformedSchemaJson = containsComment ? "/* schema comment */ " + malformedSchemaJson : malformedSchemaJson + '\u000b';
        RegistryArtifactReference registryArtifactReference = this.registerReference(
                registryClientFacade,
                topicName + "-reference",
                "gregor.avro.ArchivedContext",
                malformedReference ? malformedSchemaJson : referenceSchemaJson,
                Set.of()
        );
        RegistryVersionCoordinates coordinates = this.registerSchema(registryClientFacade, topicName + "-value", malformedReference ? writerSchemaJson : malformedSchemaJson, Set.of(registryArtifactReference));
        Headers headers = globalIdHeaders(coordinates);
        byte[] datumBytes = WebhookEnvelopeFixtures.phaseOneDatumWith((byte) 1);

        try (ApicurioWebhookEnvelopeReader apicurioWebhookEnvelopeReader = new ApicurioWebhookEnvelopeReader(this.webhookEnvelopeCodec, serdeProperties)) {
            assertFailure(() -> apicurioWebhookEnvelopeReader.read(topicName, headers, datumBytes), UNRESOLVABLE_SCHEMA);
        }
    }

    @Test
    void readsAHashOnlyReferenceWithoutTreatingTheDatumAsFramed() throws Exception {
        Map<String, Object> serdeProperties = this.headerSerdeProperties();
        RegistryClientFacade registryClientFacade = RegistryClientFacadeFactory.create(new SchemaResolverConfig(serdeProperties));
        String topicName = this.fixtureTopicName();
        Schema writerSchema = WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA;
        // The empty first string starts the unframed datum with Apicurio's magic byte.
        GenericRecord genericRecord = WebhookEnvelopeFixtures.phaseOneEvent(writerSchema, "");
        Headers headers = new RecordHeaders();
        byte[] valueBytes = serialize(serdeProperties, topicName, headers, genericRecord);
        RegistryVersionCoordinates coordinates = registryClientFacade.getVersionCoordinatesByGAV("default", topicName + "-value", null);
        headers.remove(KafkaSerdeHeaders.HEADER_VALUE_CONTENT_ID);
        headers.add(KafkaSerdeHeaders.HEADER_VALUE_CONTENT_HASH, contentHash(registryClientFacade, coordinates));

        try (ApicurioWebhookEnvelopeReader apicurioWebhookEnvelopeReader = new ApicurioWebhookEnvelopeReader(this.webhookEnvelopeCodec, serdeProperties)) {
            StoredWebhookEnvelope storedWebhookEnvelope = apicurioWebhookEnvelopeReader.read(topicName, headers, valueBytes);
            Assertions.assertThat(storedWebhookEnvelope.datumBytes()).containsExactly(WebhookEnvelopeFixtures.writeDatum(genericRecord));
            Assertions.assertThat(new Schema.Parser().parse(storedWebhookEnvelope.writerSchemaJson())).isEqualTo(writerSchema);
            WebhookEnvelopeFixtures.assertPhaseOneEvent(this.webhookEnvelopeCodec.decode(storedWebhookEnvelope), "");
        }
    }

    @Test
    void conflictingExactIdentifiersDoNotChangeSubsequentGlobalIdResolution() throws Exception {
        Map<String, Object> serdeProperties = this.headerSerdeProperties();
        RegistryClientFacade registryClientFacade = RegistryClientFacadeFactory.create(new SchemaResolverConfig(serdeProperties));
        String topicName = this.fixtureTopicName();
        Schema hashWriterSchema = WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA;
        Schema globalWriterSchema = WebhookEnvelopeFixtures.phaseOneSchemaWith(
                """
                {"name":"legacyCode","type":"int"}
                """
        );
        GenericRecord hashGenericRecord = WebhookEnvelopeFixtures.phaseOneEvent(hashWriterSchema, WebhookEnvelopeFixtures.EVENT_ID);
        GenericRecord globalGenericRecord = WebhookEnvelopeFixtures.phaseOneEvent(globalWriterSchema, WebhookEnvelopeFixtures.EVENT_ID);
        globalGenericRecord.put("legacyCode", 73);
        byte[] hashValueBytes = serialize(serdeProperties, topicName, new RecordHeaders(), hashGenericRecord);
        RegistryVersionCoordinates hashCoordinates = registryClientFacade.getVersionCoordinatesByGAV("default", topicName + "-value", null);
        byte[] globalValueBytes = serialize(serdeProperties, topicName, new RecordHeaders(), globalGenericRecord);
        RegistryVersionCoordinates globalCoordinates = registryClientFacade.getVersionCoordinatesByGAV("default", topicName + "-value", null);

        Headers globalHeaders = new RecordHeaders().add(KafkaSerdeHeaders.HEADER_VALUE_GLOBAL_ID, idBytes(globalCoordinates.getGlobalId()));
        Headers mixedHeaders = new RecordHeaders(globalHeaders).add(KafkaSerdeHeaders.HEADER_VALUE_CONTENT_HASH, contentHash(registryClientFacade, hashCoordinates));
        Headers contentIdHeaders = new RecordHeaders(mixedHeaders)
                .remove(KafkaSerdeHeaders.HEADER_VALUE_GLOBAL_ID)
                .add(KafkaSerdeHeaders.HEADER_VALUE_GLOBAL_ID, idBytes(hashCoordinates.getGlobalId()))
                .add(KafkaSerdeHeaders.HEADER_VALUE_CONTENT_ID, idBytes(globalCoordinates.getContentId()));
        try (ApicurioWebhookEnvelopeReader apicurioWebhookEnvelopeReader = new ApicurioWebhookEnvelopeReader(this.webhookEnvelopeCodec, serdeProperties)) {
            StoredWebhookEnvelope hashStoredWebhookEnvelope = apicurioWebhookEnvelopeReader.read(topicName, mixedHeaders, hashValueBytes);
            Assertions.assertThat(new Schema.Parser().parse(hashStoredWebhookEnvelope.writerSchemaJson())).isEqualTo(hashWriterSchema);
            StoredWebhookEnvelope globalStoredWebhookEnvelope = apicurioWebhookEnvelopeReader.read(topicName, globalHeaders, globalValueBytes);
            Assertions.assertThat(new Schema.Parser().parse(globalStoredWebhookEnvelope.writerSchemaJson())).isEqualTo(globalWriterSchema);
            StoredWebhookEnvelope contentIdStoredWebhookEnvelope = apicurioWebhookEnvelopeReader.read(topicName, contentIdHeaders, globalValueBytes);
            Assertions.assertThat(new Schema.Parser().parse(contentIdStoredWebhookEnvelope.writerSchemaJson())).isEqualTo(globalWriterSchema);
        }
    }

    @Test
    void rejectsMissingAndUnresolvableExactWriterReferencesWithoutFallback() {
        Map<String, Object> serdeProperties = this.headerSerdeProperties();
        String topicName = this.fixtureTopicName();
        byte[] datumBytes = serialize(serdeProperties, topicName, new RecordHeaders(), WebhookEnvelopeFixtures.phaseOneEvent(WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA, WebhookEnvelopeFixtures.EVENT_ID));
        RegistryClientFacade registryClientFacade = RegistryClientFacadeFactory.create(new SchemaResolverConfig(serdeProperties));
        RegistryVersionCoordinates coordinates = registryClientFacade.getVersionCoordinatesByGAV("default", topicName + "-value", null);
        FallbackArtifactProvider fallbackArtifactProvider = (_, _) -> ArtifactReference.builder().contentId(coordinates.getContentId()).build();
        serdeProperties.put(SerdeConfig.FALLBACK_ARTIFACT_PROVIDER, fallbackArtifactProvider);
        Headers coordinateHeaders = new RecordHeaders().add(KafkaSerdeHeaders.HEADER_VALUE_ARTIFACT_ID, (topicName + "-value").getBytes(StandardCharsets.UTF_8));
        Headers unresolvedHeaders = new RecordHeaders(coordinateHeaders).add(KafkaSerdeHeaders.HEADER_VALUE_GLOBAL_ID, idBytes(Long.MAX_VALUE));
        Headers malformedIdHeaders = new RecordHeaders().add(KafkaSerdeHeaders.HEADER_VALUE_CONTENT_ID, new byte[]{1});

        try (ApicurioWebhookEnvelopeReader apicurioWebhookEnvelopeReader = new ApicurioWebhookEnvelopeReader(this.webhookEnvelopeCodec, serdeProperties)) {
            assertFailure(() -> apicurioWebhookEnvelopeReader.read(topicName, coordinateHeaders, datumBytes), MISSING_SCHEMA);
            assertFailure(() -> apicurioWebhookEnvelopeReader.read(topicName, unresolvedHeaders, datumBytes), UNRESOLVABLE_SCHEMA);
            assertFailure(() -> apicurioWebhookEnvelopeReader.read(topicName, malformedIdHeaders, datumBytes), MALFORMED_FRAMING);
        }
    }

    @Test
    void disablesContractRulesWithoutChangingSuppliedSerdeOptions() throws Exception {
        Map<String, Object> serdeProperties = this.serdeProperties();
        String topicName = this.fixtureTopicName();
        GenericRecord genericRecord = WebhookEnvelopeFixtures.phaseOneEvent(WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA, WebhookEnvelopeFixtures.EVENT_ID);
        byte[] valueBytes = serialize(serdeProperties, topicName, new RecordHeaders(), genericRecord);
        serdeProperties.put(SerdeConfig.CONTRACT_RULES_ENABLED, true);
        // Apicurio parses the cache TTL only when contract rules are enabled.
        serdeProperties.put(SerdeConfig.CONTRACT_RULES_CACHE_TTL_SECONDS, "not-a-number");

        try (ApicurioWebhookEnvelopeReader apicurioWebhookEnvelopeReader = new ApicurioWebhookEnvelopeReader(this.webhookEnvelopeCodec, serdeProperties)) {
            StoredWebhookEnvelope storedWebhookEnvelope = apicurioWebhookEnvelopeReader.read(topicName, null, valueBytes);
            Assertions.assertThat(storedWebhookEnvelope.datumBytes()).containsExactly(WebhookEnvelopeFixtures.writeDatum(genericRecord));
            WebhookEnvelopeFixtures.assertPhaseOneEvent(this.webhookEnvelopeCodec.decode(storedWebhookEnvelope), WebhookEnvelopeFixtures.EVENT_ID);
        }
        Assertions.assertThat(serdeProperties).containsEntry(SerdeConfig.CONTRACT_RULES_ENABLED, true).containsEntry(SerdeConfig.CONTRACT_RULES_CACHE_TTL_SECONDS, "not-a-number");
    }

    @Test
    void rejectsTombstonesEmptyValuesAndMalformedPrefixes() {
        String topicName = "unused-topic";
        Headers headers = new RecordHeaders();
        assertFailure(() -> this.apicurioWebhookEnvelopeReader.read(topicName, headers, null), MALFORMED_FRAMING);
        assertFailure(() -> this.apicurioWebhookEnvelopeReader.read(topicName, headers, new byte[0]), MALFORMED_FRAMING);
        assertFailure(() -> this.apicurioWebhookEnvelopeReader.read(topicName, headers, new byte[]{0}), MALFORMED_FRAMING);
        // Check a non-magic prefix with header identifiers disabled.
        try (ApicurioWebhookEnvelopeReader prefixApicurioWebhookEnvelopeReader = new ApicurioWebhookEnvelopeReader(this.webhookEnvelopeCodec, this.serdeProperties())) {
            assertFailure(() -> prefixApicurioWebhookEnvelopeReader.read(topicName, headers, new byte[]{1, 0, 0, 0, 0}), MALFORMED_FRAMING);
        }
        assertFailure(() -> this.apicurioWebhookEnvelopeReader.read(topicName, null, new byte[]{1, 0, 0, 0, 0}), MALFORMED_FRAMING);
    }

    @Test
    void rejectsJsonEncodingInHeadersOrInTheConfiguredDefault() {
        Map<String, Object> jsonSerdeProperties = this.headerSerdeProperties();
        jsonSerdeProperties.put(AvroSerdeConfig.AVRO_ENCODING, AvroSerdeConfig.AVRO_ENCODING_JSON);
        String topicName = this.fixtureTopicName();
        Headers headers = new RecordHeaders();
        byte[] valueBytes = serialize(jsonSerdeProperties, topicName, headers, WebhookEnvelopeFixtures.phaseOneEvent(WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA, WebhookEnvelopeFixtures.EVENT_ID));
        Assertions.assertThat(new AvroSerdeHeaders(false).getEncoding(headers)).isEqualTo("JSON");
        assertFailure(() -> this.apicurioWebhookEnvelopeReader.read(topicName, headers, valueBytes), NONBINARY_ENVELOPE);

        try (ApicurioWebhookEnvelopeReader apicurioWebhookEnvelopeReader = new ApicurioWebhookEnvelopeReader(this.webhookEnvelopeCodec, jsonSerdeProperties)) {
            headers.remove(KafkaSerdeHeaders.HEADER_VALUE_ENCODING);
            assertFailure(() -> apicurioWebhookEnvelopeReader.read(topicName, headers, valueBytes), NONBINARY_ENVELOPE);
        }
    }

    @Test
    void readsBinaryEncodingHeaderDespiteJsonDefault() throws Exception {
        Map<String, Object> binarySerdeProperties = this.headerSerdeProperties();
        String topicName = this.fixtureTopicName();
        Headers headers = new RecordHeaders();
        GenericRecord genericRecord = WebhookEnvelopeFixtures.phaseOneEvent(WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA, WebhookEnvelopeFixtures.EVENT_ID);
        byte[] valueBytes = serialize(binarySerdeProperties, topicName, headers, genericRecord);
        Assertions.assertThat(new AvroSerdeHeaders(false).getEncoding(headers)).isEqualTo("BINARY");
        Map<String, Object> jsonSerdeProperties = new HashMap<>(binarySerdeProperties);
        jsonSerdeProperties.put(AvroSerdeConfig.AVRO_ENCODING, AvroSerdeConfig.AVRO_ENCODING_JSON);

        try (ApicurioWebhookEnvelopeReader apicurioWebhookEnvelopeReader = new ApicurioWebhookEnvelopeReader(this.webhookEnvelopeCodec, jsonSerdeProperties)) {
            StoredWebhookEnvelope storedWebhookEnvelope = apicurioWebhookEnvelopeReader.read(topicName, headers, valueBytes);
            Assertions.assertThat(storedWebhookEnvelope.datumBytes()).containsExactly(WebhookEnvelopeFixtures.writeDatum(genericRecord));
            WebhookEnvelopeFixtures.assertPhaseOneEvent(this.webhookEnvelopeCodec.decode(storedWebhookEnvelope), WebhookEnvelopeFixtures.EVENT_ID);

            headers.remove(KafkaSerdeHeaders.HEADER_VALUE_ENCODING);
            assertFailure(() -> apicurioWebhookEnvelopeReader.read(topicName, headers, valueBytes), NONBINARY_ENVELOPE);
        }
    }

    @Test
    void triesTheRemainingFixtureArtifactsAfterADeleteFails() {
        SequencedSet<String> artifactIds = new LinkedHashSet<>(List.of("conflict", "missing", "unavailable", "independent"));
        List<String> attemptedArtifactIds = new ArrayList<>();
        ProblemDetails conflictProblemDetails = new ProblemDetails();
        conflictProblemDetails.setStatus(HttpStatus.CONFLICT.value());
        ProblemDetails missingProblemDetails = new ProblemDetails();
        missingProblemDetails.setStatus(HttpStatus.NOT_FOUND.value());
        IllegalStateException unavailableException = new IllegalStateException("Registry unavailable");

        Assertions.assertThatIllegalStateException()
                .isThrownBy(() -> deleteFixtureArtifacts(artifactIds, artifactId -> {
                    attemptedArtifactIds.add(artifactId);
                    switch (artifactId) {
                        case "conflict" -> throw conflictProblemDetails;
                        case "missing" -> throw missingProblemDetails;
                        case "unavailable" -> throw unavailableException;
                    }
                }))
                .withMessage("Could not remove all registry fixture artifacts")
                .satisfies(exception -> Assertions.assertThat(exception.getSuppressed()).containsExactly(conflictProblemDetails, unavailableException));
        Assertions.assertThat(attemptedArtifactIds).containsExactly("conflict", "missing", "unavailable", "independent");
        Assertions.assertThat(artifactIds).containsExactly("conflict", "unavailable");
    }

    @AfterEach
    void removeFixtureArtifacts() throws Exception {
        if (this.fixtureArtifactIds.isEmpty()) {
            return;
        }
        RegistryClientFacade registryClientFacade = RegistryClientFacadeFactory.create(new SchemaResolverConfig(this.serdeProperties()));
        RegistryClient registryClient = (RegistryClient) registryClientFacade.getClient();
        WithPropertyNameItemRequestBuilder deletionProperty = registryClient.admin().config().properties().byPropertyName(ARTIFACT_DELETION_PROPERTY);
        String previousDeletionValue = deletionProperty.get().getValue();
        boolean deletionAlreadyEnabled = Boolean.parseBoolean(previousDeletionValue);
        List<String> previousDeletionOverrides = this.artifactDeletionOverrides();
        UpdateConfigurationProperty updateConfigurationProperty = new UpdateConfigurationProperty();
        try (
                AutoCloseable _ = () -> {
                    if (!deletionAlreadyEnabled) {
                        // The admin GET exposes only the effective value. PUT refreshes the 3.2.6 cache before removing a fixture-only override.
                        updateConfigurationProperty.setValue(previousDeletionOverrides.isEmpty() ? previousDeletionValue : previousDeletionOverrides.getFirst());
                        deletionProperty.put(updateConfigurationProperty);
                        if (previousDeletionOverrides.isEmpty()) {
                            deletionProperty.delete();
                        }
                    }
                    Assertions.assertThat(this.artifactDeletionOverrides()).containsExactlyElementsOf(previousDeletionOverrides);
                    Assertions.assertThat(deletionProperty.get().getValue()).isEqualTo(previousDeletionValue);
                }
        ) {
            if (!deletionAlreadyEnabled) {
                updateConfigurationProperty.setValue("true");
                deletionProperty.put(updateConfigurationProperty);
            }
            deleteFixtureArtifacts(this.fixtureArtifactIds, artifactId -> registryClient.groups().byGroupId("default").artifacts().byArtifactId(artifactId).delete());
        }
    }

    private static void deleteFixtureArtifacts(SequencedSet<String> artifactIds, Consumer<String> deleteArtifact) {
        IllegalStateException cleanupFailure = null;
        Iterator<String> artifactIdIterator = artifactIds.iterator();
        while (artifactIdIterator.hasNext()) {
            try {
                deleteArtifact.accept(artifactIdIterator.next());
            } catch (RuntimeException exception) {
                if (!(exception instanceof ProblemDetails problemDetails && Objects.equals(problemDetails.getStatus(), HttpStatus.NOT_FOUND.value()))) {
                    if (Objects.isNull(cleanupFailure)) {
                        cleanupFailure = new IllegalStateException("Could not remove all registry fixture artifacts");
                    }
                    cleanupFailure.addSuppressed(exception);
                    continue;
                }
            }
            artifactIdIterator.remove();
        }
        if (Objects.nonNull(cleanupFailure)) {
            throw cleanupFailure;
        }
    }

    private List<String> artifactDeletionOverrides() {
        return this.jdbcTemplate.queryForList("SELECT propvalue FROM apicurio_registry.config WHERE propname = ?", String.class, ARTIFACT_DELETION_PROPERTY);
    }

    private Map<String, Object> serdeProperties() {
        Map<String, Object> serdeProperties = new HashMap<>(Binder.get(this.environment).bind("camel.component.kafka.additional-properties", Bindable.mapOf(String.class, Object.class)).get());
        serdeProperties.put(SchemaResolverConfig.VERTX_INSTANCE, this.registryVertx);
        return serdeProperties;
    }

    private Map<String, Object> headerSerdeProperties() {
        Map<String, Object> serdeProperties = this.serdeProperties();
        serdeProperties.put(KafkaSerdeConfig.ENABLE_HEADERS, true);
        return serdeProperties;
    }

    private String fixtureTopicName() {
        String topicName = "gregor-occurrence-test-" + UUID.randomUUID();
        this.fixtureArtifactIds.add(topicName + "-value");
        return topicName;
    }

    private RegistryArtifactReference registerReference(RegistryClientFacade registryClientFacade, String artifactId, String fullName, String schemaJson, Set<RegistryArtifactReference> registryArtifactReferences) {
        RegistryVersionCoordinates coordinates = this.registerSchema(registryClientFacade, artifactId, schemaJson, registryArtifactReferences);
        return RegistryArtifactReference.builder().name(fullName).groupId(coordinates.getGroupId()).artifactId(artifactId).version(coordinates.getVersion()).build();
    }

    private RegistryVersionCoordinates registerSchema(RegistryClientFacade registryClientFacade, String artifactId, String schemaJson, Set<RegistryArtifactReference> registryArtifactReferences) {
        // Delete parents before their reference artifacts. Register leaves first.
        this.fixtureArtifactIds.addFirst(artifactId);
        return registryClientFacade.createSchema("AVRO", "default", artifactId, null, "CREATE_VERSION", false, schemaJson, registryArtifactReferences);
    }

    private static Headers globalIdHeaders(RegistryVersionCoordinates coordinates) {
        return new RecordHeaders().add(KafkaSerdeHeaders.HEADER_VALUE_GLOBAL_ID, idBytes(coordinates.getGlobalId()));
    }

    private static byte[] serialize(Map<String, Object> serdeProperties, String topicName, Headers headers, GenericRecord genericRecord) {
        try (AvroKafkaSerializer<GenericRecord> avroKafkaSerializer = new AvroKafkaSerializer<>()) {
            avroKafkaSerializer.configure(serdeProperties, false);
            return avroKafkaSerializer.serialize(topicName, headers, genericRecord);
        }
    }

    private static byte[] idBytes(long id) {
        return ByteBuffer.allocate(Long.BYTES).putLong(id).array();
    }

    private static byte[] contentHash(RegistryClientFacade registryClientFacade, RegistryVersionCoordinates coordinates) throws Exception {
        String schemaJson = registryClientFacade.getSchemaByContentId(coordinates.getContentId());
        byte[] digestBytes = MessageDigest.getInstance("SHA-256").digest(schemaJson.getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(digestBytes).getBytes(StandardCharsets.UTF_8);
    }

    private static void assertFailure(ThrowingCallable throwingCallable, String message) {
        Assertions.assertThatExceptionOfType(InvalidWebhookEnvelopeException.class)
                .isThrownBy(throwingCallable)
                .withMessage(message)
                .withNoCause()
                .satisfies(exception -> Assertions.assertThat(exception.getSuppressed()).isEmpty());
    }
}
