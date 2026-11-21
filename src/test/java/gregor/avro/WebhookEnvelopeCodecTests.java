package gregor.avro;

import gregor.dto.StoredWebhookEnvelope;
import gregor.exception.InvalidWebhookEnvelopeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.ClassSecurityValidator;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.context.support.GenericApplicationContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

class WebhookEnvelopeCodecTests {

    private static final String MALFORMED_DATUM = "Webhook envelope datum is malformed";
    private static final String INVALID_SCHEMA = "Webhook envelope writer schema is invalid";
    private static final String MISSING_SCHEMA = "Webhook envelope writer schema is missing";
    private static final String INCOMPATIBLE_SCHEMA = "Webhook envelope writer schema is incompatible";
    private static final String ENCODE_FAILED = "Webhook envelope could not be encoded";

    private static ClassSecurityValidator.ClassSecurityPredicate previousClassSecurityPredicate;

    private final WebhookEnvelopeCodec webhookEnvelopeCodec = new WebhookEnvelopeCodec();

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
    @NullAndEmptySource
    @ValueSource(strings = {" \t\n", "\u2003"})
    void rejectsMissingSchemaContext(String writerSchemaJson) {
        assertFailure(() -> new StoredWebhookEnvelope(writerSchemaJson, new byte[]{1}), MISSING_SCHEMA);
    }

    @Test
    void rejectsMissingDatumAndEnvelope() {
        String writerSchemaJson = WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA.toString();
        assertFailure(() -> new StoredWebhookEnvelope(writerSchemaJson, null), MALFORMED_DATUM);
        assertFailure(() -> new StoredWebhookEnvelope(writerSchemaJson, new byte[0]), MALFORMED_DATUM);
        assertFailure(() -> this.webhookEnvelopeCodec.retain(null, new byte[]{1}), MISSING_SCHEMA);
        assertFailure(() -> this.webhookEnvelopeCodec.retain(WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA, null), MALFORMED_DATUM);
        assertFailure(() -> this.webhookEnvelopeCodec.decode(null), "Webhook envelope is missing");
        assertFailure(() -> this.webhookEnvelopeCodec.encode(null), ENCODE_FAILED);
    }

    @Test
    void storedEnvelopeDefensivelyCopiesDatumAndRedactsItsStringForm() {
        byte[] datumBytes = {0x7b, 0x20, 0x7d};
        StoredWebhookEnvelope storedWebhookEnvelope = new StoredWebhookEnvelope("private-schema", datumBytes);
        datumBytes[0] = 0;
        storedWebhookEnvelope.datumBytes()[1] = 0;
        Assertions.assertThat(storedWebhookEnvelope.datumBytes()).containsExactly(0x7b, 0x20, 0x7d);
        Assertions.assertThat(storedWebhookEnvelope.toString()).isEqualTo("StoredWebhookEnvelope[redacted]");
    }

    @Test
    void storedEnvelopeCopiesHaveEqualValuesAndHashCodes() {
        StoredWebhookEnvelope storedWebhookEnvelope = new StoredWebhookEnvelope("writer-schema", new byte[]{1, 2, 3});
        StoredWebhookEnvelope copiedStoredWebhookEnvelope = new StoredWebhookEnvelope(storedWebhookEnvelope.writerSchemaJson(), storedWebhookEnvelope.datumBytes());

        Assertions.assertThat(copiedStoredWebhookEnvelope).isEqualTo(storedWebhookEnvelope).hasSameHashCodeAs(storedWebhookEnvelope);
        Assertions.assertThat(storedWebhookEnvelope).isNotEqualTo(new StoredWebhookEnvelope("writer-schema", new byte[]{1, 2, 4})).isNotEqualTo(new StoredWebhookEnvelope("other-schema", new byte[]{1, 2, 3}));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void roundTripsTerminalStateAndWritesOneUnframedDatumWithoutMovingPayload(boolean hasEligibility) throws IOException {
        Instant nextEligibleAt = hasEligibility ? WebhookEnvelopeFixtures.NEXT_ELIGIBLE_AT : null;
        WebhookEvent webhookEvent = WebhookEnvelopeFixtures.currentEvent(WebhookEnvelopeFixtures.EVENT_ID, 2, nextEligibleAt);
        ByteBuffer payloadByteBuffer = ByteBuffer.wrap(new byte[]{0x00, 0x7b, 0x20, 0x7d, 0x00}).position(1).limit(4).mark();
        webhookEvent.put(WebhookEvent.getClassSchema().getField("payload").pos(), payloadByteBuffer);

        StoredWebhookEnvelope storedWebhookEnvelope = this.webhookEnvelopeCodec.encode(webhookEvent);
        Assertions.assertThat(payloadByteBuffer.position()).isEqualTo(1);
        Assertions.assertThat(payloadByteBuffer.limit()).isEqualTo(4);
        Assertions.assertThat(payloadByteBuffer.reset().position()).isEqualTo(1);
        Assertions.assertThat(webhookEvent.getPayload()).isEqualTo(ByteBuffer.wrap(new byte[]{0x7b, 0x20, 0x7d}));

        Schema writerSchema = new Schema.Parser().parse(storedWebhookEnvelope.writerSchemaJson());
        Assertions.assertThat(writerSchema).isEqualTo(WebhookEvent.getClassSchema());
        BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(storedWebhookEnvelope.datumBytes(), null);
        GenericRecord genericRecord = new GenericDatumReader<GenericRecord>(writerSchema).read(null, binaryDecoder);
        Assertions.assertThat(genericRecord.get("payload")).isEqualTo(ByteBuffer.wrap(new byte[]{0x7b, 0x20, 0x7d}));
        Assertions.assertThat(genericRecord.get("createdAt")).isEqualTo(WebhookEnvelopeFixtures.CREATED_AT.toEpochMilli());
        Assertions.assertThat(binaryDecoder.isEnd()).isTrue();

        WebhookEvent decodedWebhookEvent = this.webhookEnvelopeCodec.decode(storedWebhookEnvelope);
        WebhookEnvelopeFixtures.assertIdentityAndPayload(decodedWebhookEvent, WebhookEnvelopeFixtures.EVENT_ID, new byte[]{0x7b, 0x20, 0x7d});
        Assertions.assertThat(decodedWebhookEvent.getNextEligibleAt()).isEqualTo(nextEligibleAt);
        Assertions.assertThat(decodedWebhookEvent.getRetryCount()).isEqualTo(3);
        Assertions.assertThat(decodedWebhookEvent.getDeliveryAttemptCount()).isEqualTo(4);
        Assertions.assertThat(decodedWebhookEvent.getReplayCount()).isEqualTo(2);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ffc08000"})
    void encodesOpaquePayloadBytesWithoutTextValidation(String payloadHex) throws IOException {
        byte[] payloadBytes = HexFormat.of().parseHex(payloadHex);
        Schema readerSchema = WebhookEvent.getClassSchema();
        WebhookEvent webhookEvent = WebhookEnvelopeFixtures.currentEvent(WebhookEnvelopeFixtures.EVENT_ID, 2, null);
        webhookEvent.put(readerSchema.getField("payload").pos(), ByteBuffer.wrap(payloadBytes.clone()));

        StoredWebhookEnvelope storedWebhookEnvelope = this.webhookEnvelopeCodec.encode(webhookEvent);
        BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(storedWebhookEnvelope.datumBytes(), null);
        GenericRecord genericRecord = new GenericDatumReader<GenericRecord>(readerSchema).read(null, binaryDecoder);
        WebhookEvent decodedWebhookEvent = this.webhookEnvelopeCodec.decode(storedWebhookEnvelope);

        Assertions.assertThat(genericRecord.get("payload")).isEqualTo(ByteBuffer.wrap(payloadBytes));
        Assertions.assertThat(decodedWebhookEvent.getPayload()).isEqualTo(ByteBuffer.wrap(payloadBytes));
    }

    @Test
    void encodesEnvelopeTimestampsAtAvroWirePrecisionWithoutMutatingTheEvent() {
        Instant createdAt = Instant.parse("1969-12-31T23:59:59.999876Z");
        Instant nextEligibleAt = Instant.parse("2026-07-10T12:00:00.456789Z");
        WebhookEvent webhookEvent = WebhookEnvelopeFixtures.currentEvent(WebhookEnvelopeFixtures.EVENT_ID, 2, nextEligibleAt);
        webhookEvent.put(WebhookEvent.getClassSchema().getField("createdAt").pos(), createdAt);

        WebhookEvent decodedWebhookEvent = this.webhookEnvelopeCodec.decode(this.webhookEnvelopeCodec.encode(webhookEvent));

        Assertions.assertThat(decodedWebhookEvent.getCreatedAt()).isEqualTo(Instant.parse("1969-12-31T23:59:59.999Z"));
        Assertions.assertThat(decodedWebhookEvent.getNextEligibleAt()).isEqualTo(Instant.parse("2026-07-10T12:00:00.456Z"));
        Assertions.assertThat(webhookEvent.getCreatedAt()).isEqualTo(createdAt);
        Assertions.assertThat(webhookEvent.getNextEligibleAt()).isEqualTo(nextEligibleAt);
    }

    @Test
    void decodesActualPhaseOneDatumWithOnlyTheReaderDefaults() throws IOException {
        Schema writerSchema = WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA;
        WebhookEvent webhookEvent = this.webhookEnvelopeCodec.decode(new StoredWebhookEnvelope(writerSchema.toString(), WebhookEnvelopeFixtures.phaseOneDatumWith()));
        WebhookEnvelopeFixtures.assertPhaseOneEvent(webhookEvent, WebhookEnvelopeFixtures.EVENT_ID);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}",
            "[{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"},\"null\"]"
    })
    void resolvesCompatibleEligibilityTypesAgainstTheCurrentReaderUnion(String eligibilityTypeJson) throws IOException {
        Schema writerSchema = WebhookEnvelopeFixtures.phaseOneSchemaWith("{\"name\":\"nextEligibleAt\",\"type\":" + eligibilityTypeJson + "}");
        GenericRecord genericRecord = WebhookEnvelopeFixtures.phaseOneEvent(writerSchema, WebhookEnvelopeFixtures.EVENT_ID);
        genericRecord.put("nextEligibleAt", WebhookEnvelopeFixtures.NEXT_ELIGIBLE_AT.toEpochMilli());
        byte[] datumBytes = WebhookEnvelopeFixtures.writeDatum(genericRecord);

        StoredWebhookEnvelope storedWebhookEnvelope = this.webhookEnvelopeCodec.retain(writerSchema, datumBytes);
        WebhookEvent webhookEvent = this.webhookEnvelopeCodec.decode(storedWebhookEnvelope);

        Assertions.assertThat(storedWebhookEnvelope.datumBytes()).containsExactly(datumBytes);
        WebhookEnvelopeFixtures.assertIdentityAndPayload(webhookEvent, WebhookEnvelopeFixtures.EVENT_ID, new byte[]{0x7b, 0x20, 0x7d});
        Assertions.assertThat(webhookEvent.getNextEligibleAt()).isEqualTo(WebhookEnvelopeFixtures.NEXT_ELIGIBLE_AT);
        Assertions.assertThat(webhookEvent.getRetryCount()).isEqualTo(2);
        Assertions.assertThat(webhookEvent.getDeliveryAttemptCount()).isZero();
        Assertions.assertThat(webhookEvent.getReplayCount()).isZero();
    }

    @Test
    void retainsAlternateArrayBlocksAndDoesNotInterpretOpaqueBytesAsText() throws IOException {
        Schema writerSchema = WebhookEnvelopeFixtures.phaseOneSchemaWith(
                """
                {"name":"writerItems","type":{"type":"array","items":"int"}}
                """
        );
        GenericRecord genericRecord = WebhookEnvelopeFixtures.phaseOneEvent(WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA, WebhookEnvelopeFixtures.EVENT_ID);
        byte[] payloadBytes = HexFormat.of().parseHex("ffc08000");
        genericRecord.put("payload", ByteBuffer.wrap(payloadBytes));
        // A sized block of two ints, an unsized block of one int, then the terminator.
        byte[] datumBytes = WebhookEnvelopeFixtures.appendBytes(WebhookEnvelopeFixtures.writeDatum(genericRecord), HexFormat.of().parseHex("03040204020600"));
        byte[] expectedDatumBytes = datumBytes.clone();
        StoredWebhookEnvelope storedWebhookEnvelope = this.webhookEnvelopeCodec.retain(writerSchema, datumBytes);
        Arrays.fill(datumBytes, (byte) 0);

        Assertions.assertThat(storedWebhookEnvelope.datumBytes()).containsExactly(expectedDatumBytes);
        Schema retainedSchema = new Schema.Parser().parse(storedWebhookEnvelope.writerSchemaJson());
        BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(storedWebhookEnvelope.datumBytes(), null);
        GenericRecord retainedGenericRecord = new GenericDatumReader<GenericRecord>(retainedSchema).read(null, binaryDecoder);
        Assertions.assertThat((List<?>) retainedGenericRecord.get("writerItems")).isEqualTo(List.of(1, 2, 3));
        WebhookEnvelopeFixtures.assertIdentityAndPayload(this.webhookEnvelopeCodec.decode(storedWebhookEnvelope), WebhookEnvelopeFixtures.EVENT_ID, payloadBytes);
    }

    @ParameterizedTest
    @ValueSource(strings = {"808080801000", "ffffffff0f0000"})
    void projectsLargeZeroWidthWriterArrayInEitherBlockLayout(String blockHex) throws IOException {
        String phaseOneSchemaJson = WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA.toString();
        Schema writerSchema = new Schema.Parser().parse(phaseOneSchemaJson.replace(
                "{\"name\":\"retryCount\"",
                """
                {"name":"writerItems","type":{"type":"array","items":"null"}},{"name":"retryCount"
                """.stripTrailing()
        ));
        byte[] phaseOneDatumBytes = WebhookEnvelopeFixtures.phaseOneDatumWith();
        // Insert one block of 2^31 nulls before the final single-byte retry count.
        byte[] blockBytes = HexFormat.of().parseHex(blockHex);
        byte[] datumBytes = Arrays.copyOf(phaseOneDatumBytes, phaseOneDatumBytes.length + blockBytes.length);
        int retryCountOffset = phaseOneDatumBytes.length - 1;
        System.arraycopy(blockBytes, 0, datumBytes, retryCountOffset, blockBytes.length);
        datumBytes[datumBytes.length - 1] = phaseOneDatumBytes[retryCountOffset];

        StoredWebhookEnvelope storedWebhookEnvelope = this.webhookEnvelopeCodec.retain(writerSchema, datumBytes);
        Assertions.assertThat(storedWebhookEnvelope.datumBytes()).containsExactly(datumBytes);
        WebhookEnvelopeFixtures.assertPhaseOneEvent(this.webhookEnvelopeCodec.decode(storedWebhookEnvelope), WebhookEnvelopeFixtures.EVENT_ID);
    }

    @Test
    void projectsReusedZeroWidthRecordsWithoutSkippingConsumingFields() throws IOException {
        Schema writerSchema = WebhookEnvelopeFixtures.phaseOneSchemaWith(
                """
                {"name":"writerItems","type":{"type":"array","items":{"type":"record","name":"Item","fields":[
                    {"name":"tree","type":%s},{"name":"verified","type":"boolean"}
                ]}}}
                """.formatted(zeroWidthTreeSchemaJson())
        );
        // The two items' trees are empty on the wire. Only the block count, booleans, and terminator are encoded.
        byte[] datumBytes = WebhookEnvelopeFixtures.phaseOneDatumWith(HexFormat.of().parseHex("04010000"));

        org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            StoredWebhookEnvelope storedWebhookEnvelope = this.webhookEnvelopeCodec.retain(writerSchema, datumBytes);
            Assertions.assertThat(storedWebhookEnvelope.datumBytes()).containsExactly(datumBytes);
            Assertions.assertThat(new Schema.Parser().parse(storedWebhookEnvelope.writerSchemaJson())).isEqualTo(writerSchema);
            WebhookEnvelopeFixtures.assertPhaseOneEvent(this.webhookEnvelopeCodec.decode(storedWebhookEnvelope), WebhookEnvelopeFixtures.EVENT_ID);
        });

        datumBytes[datumBytes.length - 2] = 2;
        this.assertMalformed(writerSchema, datumBytes);
    }

    @Test
    void retainsDeepRecursiveWriterOnlyDataAndChecksEveryField() throws IOException {
        Schema writerSchema = WebhookEnvelopeFixtures.phaseOneSchemaWith(
                """
                {"name":"writerChain","type":["null",{"type":"record","name":"Link","fields":[
                    {"name":"next","type":["null","Link"]},{"name":"verified","type":"boolean"}
                ]}]}
                """
        );
        int depth = 50_000;
        byte[] chainBytes = new byte[2 * depth + 1];
        Arrays.fill(chainBytes, 0, depth, (byte) 2); // Select Link until the innermost null.
        Arrays.fill(chainBytes, depth + 1, chainBytes.length, (byte) 1); // One true boolean per Link.
        byte[] datumBytes = WebhookEnvelopeFixtures.phaseOneDatumWith(chainBytes);

        StoredWebhookEnvelope storedWebhookEnvelope = this.webhookEnvelopeCodec.retain(writerSchema, datumBytes);
        Assertions.assertThat(storedWebhookEnvelope.datumBytes()).containsExactly(datumBytes);
        Assertions.assertThat(new Schema.Parser().parse(storedWebhookEnvelope.writerSchemaJson())).isEqualTo(writerSchema);
        WebhookEnvelopeFixtures.assertPhaseOneEvent(this.webhookEnvelopeCodec.decode(storedWebhookEnvelope), WebhookEnvelopeFixtures.EVENT_ID);

        chainBytes[depth + 1] = 2; // The innermost Link writes its boolean first, immediately after the null index.
        this.assertMalformed(writerSchema, WebhookEnvelopeFixtures.phaseOneDatumWith(chainBytes));
    }

    @Test
    void rejectsNonterminatingRecordWithoutConsumingDatumBytes() throws IOException {
        Schema writerSchema = WebhookEnvelopeFixtures.phaseOneSchemaWith(
                """
                {"name":"writerLoop","type":{"type":"record","name":"Loop","fields":[{"name":"next","type":"Loop"}]}}
                """
        );
        this.assertMalformed(writerSchema, WebhookEnvelopeFixtures.phaseOneDatumWith());
    }

    @Test
    void retainsSizedMapBlockWithUtf8KeyAndOpaqueFixedValue() throws IOException {
        Schema writerSchema = WebhookEnvelopeFixtures.phaseOneSchemaWith(
                """
                {"name":"writerMap","type":{"type":"map","values":{"type":"fixed","name":"OpaquePair","size":2}}}
                """
        );
        byte[] datumBytes = WebhookEnvelopeFixtures.phaseOneDatumWith(HexFormat.of().parseHex("010e08f09f9a80ff8000"));
        StoredWebhookEnvelope storedWebhookEnvelope = this.webhookEnvelopeCodec.retain(writerSchema, datumBytes);
        Assertions.assertThat(storedWebhookEnvelope.datumBytes()).containsExactly(datumBytes);
        BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(storedWebhookEnvelope.datumBytes(), null);
        GenericRecord genericRecord = new GenericDatumReader<GenericRecord>(writerSchema).read(null, binaryDecoder);
        Map<?, ?> writerValues = (Map<?, ?>) genericRecord.get("writerMap");
        Assertions.assertThat(writerValues.keySet()).extracting(Object::toString).containsExactly("\uD83D\uDE80");
        Assertions.assertThat(((GenericFixed) writerValues.values().iterator().next()).bytes()).containsExactly(0xff, 0x80);
        WebhookEnvelopeFixtures.assertPhaseOneEvent(this.webhookEnvelopeCodec.decode(storedWebhookEnvelope), WebhookEnvelopeFixtures.EVENT_ID);
    }

    @Test
    void retainsNestedSizedBlocksAndContinuesIntoFollowingBlocks() throws IOException {
        Schema writerSchema = WebhookEnvelopeFixtures.phaseOneSchemaWith(
                """
                {"name":"writerMap","type":{"type":"map","values":{"type":"array","items":"int"}}}
                """
        );
        // A sized map block for a=[1,2] with two array blocks, then an unsized block for b=[].
        byte[] datumBytes = WebhookEnvelopeFixtures.phaseOneDatumWith(HexFormat.of().parseHex("011002610102020204000202620000"));
        StoredWebhookEnvelope storedWebhookEnvelope = this.webhookEnvelopeCodec.retain(writerSchema, datumBytes);
        Assertions.assertThat(storedWebhookEnvelope.datumBytes()).containsExactly(datumBytes);
        WebhookEnvelopeFixtures.assertPhaseOneEvent(this.webhookEnvelopeCodec.decode(storedWebhookEnvelope), WebhookEnvelopeFixtures.EVENT_ID);
    }

    @ParameterizedTest
    @ValueSource(strings = {"eventId", "eventType", "tenantId", "endpointId"})
    void rejectsUnpairedSurrogatesBeforeEncoding(String fieldName) {
        WebhookEvent webhookEvent = WebhookEnvelopeFixtures.currentEvent(WebhookEnvelopeFixtures.EVENT_ID, 2, null);
        webhookEvent.put(WebhookEvent.getClassSchema().getField(fieldName).pos(), "private-text-\uD800");
        assertFailure(() -> this.webhookEnvelopeCodec.encode(webhookEvent), ENCODE_FAILED);
    }

    @Test
    void rejectsSerializationFailureWithNoCause() {
        WebhookEvent webhookEvent = WebhookEnvelopeFixtures.currentEvent(WebhookEnvelopeFixtures.EVENT_ID, 2, null);
        webhookEvent.put(WebhookEvent.getClassSchema().getField("payload").pos(), null);
        assertFailure(() -> this.webhookEnvelopeCodec.encode(webhookEvent), ENCODE_FAILED);
    }

    @Test
    void rejectsInvalidStoredSchemaAndIncompleteWriterSchema() {
        StoredWebhookEnvelope storedWebhookEnvelope = new StoredWebhookEnvelope("{private-schema-fragment", new byte[]{1});
        assertFailure(() -> this.webhookEnvelopeCodec.decode(storedWebhookEnvelope), INVALID_SCHEMA);
        Schema incompleteSchema = Schema.createRecord("WebhookEvent", null, "gregor.avro", false);
        assertFailure(() -> this.webhookEnvelopeCodec.retain(incompleteSchema, new byte[]{1}), INVALID_SCHEMA);
    }

    @ParameterizedTest
    @ValueSource(strings = {"\013", "\f", "\000", "null"})
    void rejectsInvalidTrailingContentInStoredWriterSchema(String suffix) throws IOException {
        String writerSchemaJson = WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA + suffix;
        StoredWebhookEnvelope storedWebhookEnvelope = new StoredWebhookEnvelope(writerSchemaJson, WebhookEnvelopeFixtures.phaseOneDatumWith());
        assertFailure(() -> this.webhookEnvelopeCodec.decode(storedWebhookEnvelope), INVALID_SCHEMA);
    }

    @Test
    void rejectsCommentsInStoredWriterSchema() throws IOException {
        String writerSchemaJson = "/* private schema comment */" + WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA;
        StoredWebhookEnvelope storedWebhookEnvelope = new StoredWebhookEnvelope(writerSchemaJson, WebhookEnvelopeFixtures.phaseOneDatumWith());
        assertFailure(() -> this.webhookEnvelopeCodec.decode(storedWebhookEnvelope), INVALID_SCHEMA);
    }

    @Test
    void acceptsJsonWhitespaceAroundStoredWriterSchema() throws IOException {
        String writerSchemaJson = " \t\n\r" + WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA + " \t\n\r";
        StoredWebhookEnvelope storedWebhookEnvelope = new StoredWebhookEnvelope(writerSchemaJson, WebhookEnvelopeFixtures.phaseOneDatumWith());
        WebhookEnvelopeFixtures.assertPhaseOneEvent(this.webhookEnvelopeCodec.decode(storedWebhookEnvelope), WebhookEnvelopeFixtures.EVENT_ID);
    }

    @Test
    void rejectsStoredWriterSchemaWithAnUnresolvedNamedType() throws IOException {
        String writerSchemaJson = WebhookEnvelopeFixtures.phaseOneSchemaJsonWith("{\"name\":\"writerContext\",\"type\":\"gregor.avro.UnavailableWriterContext\"}");
        StoredWebhookEnvelope storedWebhookEnvelope = new StoredWebhookEnvelope(writerSchemaJson, WebhookEnvelopeFixtures.phaseOneDatumWith());
        assertFailure(() -> this.webhookEnvelopeCodec.decode(storedWebhookEnvelope), INVALID_SCHEMA);
    }

    @ParameterizedTest
    @ValueSource(strings = {"\"doc\":\"Different detail\",", "\"aliases\":[\"LegacyDetail\"],"})
    void rejectsConflictingNamedDefinitionsInStoredSchema(String conflictingMetadataJson) throws IOException {
        String writerSchemaJson = WebhookEnvelopeFixtures.phaseOneSchemaJsonWith(
                """
                {"name":"firstDetail","type":{"type":"record","name":"Detail","fields":[{"name":"verified","type":"boolean"}]}},
                {"name":"secondDetail","type":{"type":"record","name":"Detail",%s"fields":[{"name":"verified","type":"boolean"}]}}
                """.formatted(conflictingMetadataJson)
        );
        StoredWebhookEnvelope storedWebhookEnvelope = new StoredWebhookEnvelope(writerSchemaJson, WebhookEnvelopeFixtures.phaseOneDatumWith((byte) 1, (byte) 0));
        assertFailure(() -> this.webhookEnvelopeCodec.decode(storedWebhookEnvelope), INVALID_SCHEMA);
    }

    @Test
    void retainsEmptyNamespaceAliasesWithoutChangingTheirMeaning() throws IOException {
        Schema writerSchema = WebhookEnvelopeFixtures.phaseOneSchemaWith(
                """
                {"name":"writerContext","type":{"type":"record","name":"SavedContext","aliases":[".LegacyContext","LegacyContext","archive.OlderContext"],
                    "fields":[{"name":"verified","type":"boolean"}]}}
                """
        );
        byte[] datumBytes = WebhookEnvelopeFixtures.phaseOneDatumWith((byte) 1);

        StoredWebhookEnvelope storedWebhookEnvelope = this.webhookEnvelopeCodec.retain(writerSchema, datumBytes);

        Schema retainedSchema = new Schema.Parser().parse(storedWebhookEnvelope.writerSchemaJson());
        Assertions.assertThat(retainedSchema.getField("writerContext").schema().getAliases()).containsExactlyInAnyOrder(".LegacyContext", "gregor.avro.LegacyContext", "archive.OlderContext");
        Assertions.assertThat(storedWebhookEnvelope.datumBytes()).containsExactly(datumBytes);
        WebhookEnvelopeFixtures.assertPhaseOneEvent(this.webhookEnvelopeCodec.decode(storedWebhookEnvelope), WebhookEnvelopeFixtures.EVENT_ID);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "\"fields\":[{\"name\":\"other\",\"type\":\"boolean\"}]",
            "\"fields\":[{\"name\":\"first\",\"type\":\"boolean\"}],\"aliases\":[\"LegacyDetail\"]",
            "\"fields\":[{\"name\":\"first\",\"type\":\"boolean\"}],\"doc\":\"Legacy detail\""
    })
    void rejectsSnapshotThatCollapsesDifferentDefinitionsOfTheSameNamedRecord(String conflictingDefinitionJson) throws IOException {
        String detailSchemaJson = "{\"type\":\"record\",\"name\":\"gregor.avro.Detail\",%s}";
        Schema firstSchema = new Schema.Parser().parse(detailSchemaJson.formatted("\"fields\":[{\"name\":\"first\",\"type\":\"boolean\"}]"));
        Schema secondSchema = new Schema.Parser().parse(detailSchemaJson.formatted(conflictingDefinitionJson));
        List<Schema.Field> fields = new ArrayList<>();
        for (Schema.Field field : WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA.getFields()) {
            fields.add(new Schema.Field(field, field.schema()));
        }
        fields.add(new Schema.Field("firstDetail", firstSchema));
        fields.add(new Schema.Field("secondDetail", secondSchema));
        Schema writerSchema = Schema.createRecord("WebhookEvent", null, "gregor.avro", false, fields);
        byte[] datumBytes = WebhookEnvelopeFixtures.phaseOneDatumWith((byte) 1, (byte) 0);
        assertFailure(() -> this.webhookEnvelopeCodec.retain(writerSchema, datumBytes), INVALID_SCHEMA);
    }

    @ParameterizedTest
    @MethodSource("incompatibleSchemas")
    void rejectsIncompatibleWriterSchemaEvenWhenAvroAllowsPromotion(Schema writerSchema) {
        assertFailure(() -> this.webhookEnvelopeCodec.retain(writerSchema, new byte[]{1}), INCOMPATIBLE_SCHEMA);
        assertFailure(() -> this.webhookEnvelopeCodec.decode(new StoredWebhookEnvelope(writerSchema.toString(), new byte[]{1})), INCOMPATIBLE_SCHEMA);
    }

    private static Stream<Schema> incompatibleSchemas() {
        String phaseOneSchemaJson = WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA.toString();
        return Stream.of(
                new Schema.Parser().parse("\"string\""),
                new Schema.Parser().parse(phaseOneSchemaJson.replace("gregor.avro", "different.avro")),
                new Schema.Parser().parse(phaseOneSchemaJson.replace("timestamp-millis", "timestamp-micros")),
                new Schema.Parser().parse(phaseOneSchemaJson.replace("\"endpointId\",\"type\":\"string\"", "\"endpointId\",\"type\":\"bytes\"")),
                new Schema.Parser().parse(phaseOneSchemaJson.replace("\"endpointId\",\"type\":\"string\"", "\"endpointId\",\"type\":[\"string\",\"bytes\"]")),
                new Schema.Parser().parse(phaseOneSchemaJson.replace("\"payload\",\"type\":\"bytes\"", "\"payload\",\"type\":\"string\"")),
                new Schema.Parser().parse(phaseOneSchemaJson.replace("\"name\":\"endpointId\"", "\"name\":\"otherId\"")),
                WebhookEnvelopeFixtures.phaseOneSchemaWith(
                        """
                        {"name":"nextEligibleAt","type":["null","long"],"default":null}
                        """
                )
        );
    }

    @ParameterizedTest(name = "rejects {0}")
    @MethodSource("malformedWriterOnlyFields")
    void rejectsMalformedWriterOnlyFields(String description, String writerTypeJson, String fieldHex) throws IOException {
        Schema writerSchema = WebhookEnvelopeFixtures.phaseOneSchemaWith("{\"name\":\"privateWriterField\",\"type\":" + writerTypeJson + "}");
        byte[] datumBytes = WebhookEnvelopeFixtures.phaseOneDatumWith(HexFormat.of().parseHex(fieldHex));
        this.assertMalformed(writerSchema, datumBytes);
    }

    private static Stream<Arguments> malformedWriterOnlyFields() {
        return Stream.of(
                Arguments.of("a nonboolean byte", "\"boolean\"", "02"),
                Arguments.of("an overflowing int", "\"int\"", "8080808010"),
                Arguments.of("an overflowing long", "\"long\"", "80808080808080808002"),
                Arguments.of("a negative bytes length", "\"bytes\"", "01"),
                Arguments.of("a truncated fixed field", "{\"type\":\"fixed\",\"name\":\"TwoBytes\",\"size\":2}", "ff"),
                Arguments.of("an invalid enum index", "{\"type\":\"enum\",\"name\":\"Option\",\"symbols\":[\"ONE\"]}", "02"),
                Arguments.of("an invalid union index", "[\"null\",\"int\"]", "04"),
                Arguments.of("invalid UTF-8 in a string", "\"string\"", "04c080"),
                Arguments.of("invalid UTF-8 in a map key", "{\"type\":\"map\",\"values\":\"null\"}", "0202ff00"),
                Arguments.of("an incorrect map block size", "{\"type\":\"map\",\"values\":\"boolean\"}", "010402610100"),
                Arguments.of("an incorrect array block size", "{\"type\":\"array\",\"items\":\"int\"}", "01040200")
        );
    }

    @Test
    void rejectsMalformedSharedTextTruncationAndTrailingBytes() throws IOException {
        Schema writerSchema = WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA;
        byte[] datumBytes = WebhookEnvelopeFixtures.phaseOneDatumWith();
        this.assertMalformed(writerSchema, Arrays.copyOf(datumBytes, datumBytes.length - 1));
        this.assertMalformed(writerSchema, WebhookEnvelopeFixtures.appendBytes(datumBytes, new byte[]{0}));
        // The datum starts with the single-byte Avro length of a 36-byte UUID string.
        datumBytes[1] = (byte) 0xff;
        this.assertMalformed(writerSchema, datumBytes);
    }

    private void assertMalformed(Schema writerSchema, byte[] datumBytes) {
        assertFailure(() -> this.webhookEnvelopeCodec.retain(writerSchema, datumBytes), MALFORMED_DATUM);
        assertFailure(() -> this.webhookEnvelopeCodec.decode(new StoredWebhookEnvelope(writerSchema.toString(), datumBytes)), MALFORMED_DATUM);
    }

    private static String zeroWidthTreeSchemaJson() {
        String treeSchemaJson = """
                                {"type":"record","name":"Leaf","fields":[{"name":"empty","type":"null"}]}
                                """;
        // Each level doubles the number of encoded leaf instances without adding any bytes.
        String childType = "Leaf";
        for (int level = 1; level <= 25; level++) {
            String parentType = "Branch" + level;
            treeSchemaJson = """
                             {"type":"record","name":"%s","fields":[{"name":"left","type":%s},{"name":"right","type":"%s"}]}
                             """.formatted(parentType, treeSchemaJson, childType);
            childType = parentType;
        }
        return treeSchemaJson;
    }

    private static void assertFailure(ThrowingCallable throwingCallable, String message) {
        Assertions.assertThatExceptionOfType(InvalidWebhookEnvelopeException.class)
                .isThrownBy(throwingCallable)
                .isInstanceOf(IllegalArgumentException.class)
                .withMessage(message)
                .withNoCause()
                .satisfies(exception -> Assertions.assertThat(exception.getSuppressed()).isEmpty());
    }
}
