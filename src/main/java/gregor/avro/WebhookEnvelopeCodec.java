package gregor.avro;

import gregor.dto.StoredWebhookEnvelope;
import gregor.exception.InvalidWebhookEnvelopeException;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Component
public class WebhookEnvelopeCodec {

    private static final String INVALID_SCHEMA = "Webhook envelope writer schema is invalid";
    private static final String MALFORMED_DATUM = "Webhook envelope datum is malformed";

    public StoredWebhookEnvelope encode(WebhookEvent webhookEvent) {
        try {
            CharsetEncoder charsetEncoder = StandardCharsets.UTF_8.newEncoder();
            if (Objects.isNull(webhookEvent)
                    || !charsetEncoder.canEncode(webhookEvent.getEventId())
                    || !charsetEncoder.canEncode(webhookEvent.getEventType())
                    || !charsetEncoder.canEncode(webhookEvent.getTenantId())
                    || !charsetEncoder.canEncode(webhookEvent.getEndpointId())) {
                throw new IllegalArgumentException();
            }
            Schema writerSchema = WebhookEvent.getClassSchema();
            SpecificDatumWriter<WebhookEvent> specificDatumWriter = new SpecificDatumWriter<>(writerSchema, SpecificData.getForClass(WebhookEvent.class));
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
            specificDatumWriter.write(webhookEvent, binaryEncoder);
            binaryEncoder.flush();
            return new StoredWebhookEnvelope(writerSchema.toString(), byteArrayOutputStream.toByteArray());
        } catch (IOException | RuntimeException exception) {
            throw new InvalidWebhookEnvelopeException("Webhook envelope could not be encoded");
        }
    }

    public StoredWebhookEnvelope retain(Schema writerSchema, byte[] datumBytes) {
        if (Objects.isNull(writerSchema)) {
            throw new InvalidWebhookEnvelopeException("Webhook envelope writer schema is missing");
        }
        if (Objects.isNull(datumBytes) || datumBytes.length == 0) {
            throw new InvalidWebhookEnvelopeException(MALFORMED_DATUM);
        }
        byte[] snapshotBytes = datumBytes.clone();
        WebhookEnvelopeSchemas.Snapshot snapshot;
        try {
            snapshot = WebhookEnvelopeSchemas.snapshot(writerSchema);
        } catch (RuntimeException | StackOverflowError exception) {
            throw new InvalidWebhookEnvelopeException(INVALID_SCHEMA);
        }
        this.decodeDatum(snapshot.schema(), snapshotBytes);
        return new StoredWebhookEnvelope(snapshot.schemaJson(), snapshotBytes);
    }

    public WebhookEvent decode(StoredWebhookEnvelope storedWebhookEnvelope) {
        if (Objects.isNull(storedWebhookEnvelope)) {
            throw new InvalidWebhookEnvelopeException("Webhook envelope is missing");
        }
        return this.decodeDatum(parse(storedWebhookEnvelope.writerSchemaJson()), storedWebhookEnvelope.datumBytes());
    }

    private static Schema parse(String writerSchemaJson) {
        try {
            return WebhookEnvelopeSchemas.parseSnapshot(writerSchemaJson);
        } catch (RuntimeException | StackOverflowError exception) {
            throw new InvalidWebhookEnvelopeException(INVALID_SCHEMA);
        }
    }

    private WebhookEvent decodeDatum(Schema writerSchema, byte[] datumBytes) {
        Schema readerSchema = WebhookEvent.getClassSchema();
        boolean compatible;
        try {
            compatible = WebhookEnvelopeDiagnostics.withoutAvroSchemaLogging(() -> isCompatible(writerSchema, readerSchema));
        } catch (RuntimeException | StackOverflowError exception) {
            throw new InvalidWebhookEnvelopeException(INVALID_SCHEMA);
        }
        if (!compatible) {
            throw new InvalidWebhookEnvelopeException("Webhook envelope writer schema is incompatible");
        }

        try {
            DatumProjection datumProjection = new BinaryDatumValidator(ByteBuffer.wrap(datumBytes)).validateAndProject(writerSchema, readerSchema);
            BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(datumProjection.datumBytes(), null);
            SpecificDatumReader<WebhookEvent> specificDatumReader = new SpecificDatumReader<>(datumProjection.writerSchema(), readerSchema, SpecificData.getForClass(WebhookEvent.class));
            WebhookEvent webhookEvent = specificDatumReader.read(null, binaryDecoder);
            if (Objects.nonNull(webhookEvent) && binaryDecoder.isEnd()) {
                return webhookEvent;
            }
        } catch (IOException | RuntimeException | StackOverflowError exception) {
            throw new InvalidWebhookEnvelopeException(MALFORMED_DATUM);
        }
        throw new InvalidWebhookEnvelopeException(MALFORMED_DATUM);
    }

    private static boolean isCompatible(Schema writerSchema, Schema readerSchema) {
        if (!Objects.equals(writerSchema.getType(), Schema.Type.RECORD)
                || !Objects.equals(readerSchema.getFullName(), writerSchema.getFullName())
                || !Objects.equals(SchemaCompatibility.checkReaderWriterCompatibility(readerSchema, writerSchema).getType(), SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE)) {
            return false;
        }
        for (Schema.Field readerField : readerSchema.getFields()) {
            Schema.Field writerField = writerSchema.getField(readerField.name());
            if (Objects.isNull(writerField)) {
                continue;
            }
            List<Schema> readerBranches = branches(readerField.schema());
            for (Schema writerBranch : branches(writerField.schema())) {
                boolean matches = readerBranches
                        .stream()
                        .anyMatch(readerBranch -> Objects.equals(readerBranch.getType(), writerBranch.getType())
                                && (Objects.isNull(readerBranch.getLogicalType()) || Objects.nonNull(writerBranch.getLogicalType()) && Objects.equals(readerBranch.getLogicalType().getName(), writerBranch.getLogicalType().getName())));
                if (!matches) {
                    return false;
                }
            }
        }
        return true;
    }

    private static List<Schema> branches(Schema schema) {
        return Objects.equals(schema.getType(), Schema.Type.UNION) ? schema.getTypes() : List.of(schema);
    }

    private record DatumProjection(Schema writerSchema, byte[] datumBytes) {
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static final class BinaryDatumValidator {

        private final ByteBuffer byteBuffer;
        private final CharsetDecoder charsetDecoder = StandardCharsets.UTF_8.newDecoder();
        private final Map<Schema, Integer> recordOffsets = new IdentityHashMap<>();
        private final Set<Schema> zeroWidthRecords = Collections.newSetFromMap(new IdentityHashMap<>());

        private DatumProjection validateAndProject(Schema writerSchema, Schema readerSchema) throws IOException {
            boolean hasWriterOnlyFields = writerSchema.getFields().stream().anyMatch(field -> Objects.isNull(readerSchema.getField(field.name())));
            if (!hasWriterOnlyFields) {
                this.read(writerSchema);
                this.requireEndOfInput();
                return new DatumProjection(writerSchema, this.byteBuffer.array());
            }

            // Validate all fields, then give the typed reader only the shared spans. Avro's skip path applies collection allocation limits.
            List<Schema.Field> projectedFields = new ArrayList<>();
            ByteArrayOutputStream projectedByteArrayOutputStream = new ByteArrayOutputStream();
            for (Schema.Field field : writerSchema.getFields()) {
                int fieldStart = this.byteBuffer.position();
                this.read(field.schema());
                if (Objects.nonNull(readerSchema.getField(field.name()))) {
                    projectedFields.add(new Schema.Field(field, field.schema()));
                    projectedByteArrayOutputStream.write(this.byteBuffer.array(), fieldStart, this.byteBuffer.position() - fieldStart);
                }
            }
            this.requireEndOfInput();
            Schema projectedWriterSchema = Schema.createRecord(writerSchema.getName(), null, writerSchema.getNamespace(), writerSchema.isError(), projectedFields);
            return new DatumProjection(projectedWriterSchema, projectedByteArrayOutputStream.toByteArray());
        }

        private void requireEndOfInput() throws IOException {
            if (this.byteBuffer.hasRemaining()) {
                throw new IOException();
            }
        }

        private void read(Schema schema) throws IOException {
            // Datum depth can grow independently of schema depth. Keep pending fields and blocks off the Java call stack.
            Deque<ReadFrame> readFrames = new ArrayDeque<>();
            while (Objects.nonNull(schema) || !readFrames.isEmpty()) {
                if (Objects.nonNull(schema)) {
                    schema = this.readValue(schema, readFrames);
                } else {
                    schema = readFrames.peek().nextSchema();
                    if (Objects.isNull(schema)) {
                        readFrames.pop();
                    }
                }
            }
        }

        private Schema readValue(Schema schema, Deque<ReadFrame> readFrames) throws IOException {
            switch (schema.getType()) {
                case RECORD -> {
                    if (!this.zeroWidthRecords.contains(schema)) {
                        readFrames.push(new RecordFrame(schema));
                    }
                }
                case UNION -> {
                    return schema.getTypes().get(this.readIndex(schema.getTypes().size()));
                }
                case ENUM -> this.readIndex(schema.getEnumSymbols().size());
                case ARRAY -> readFrames.push(new CollectionFrame(schema.getElementType(), false));
                case MAP -> readFrames.push(new CollectionFrame(schema.getValueType(), true));
                case FIXED -> this.skip(schema.getFixedSize());
                case BYTES -> this.skip(this.readLength());
                case STRING -> this.readText();
                case INT -> this.readInteger(Integer.SIZE);
                case LONG -> this.readInteger(Long.SIZE);
                case BOOLEAN -> {
                    if (Byte.toUnsignedInt(this.byteBuffer.get()) > 1) {
                        throw new IOException();
                    }
                }
                case FLOAT -> this.skip(Float.BYTES);
                case DOUBLE -> this.skip(Double.BYTES);
                case NULL -> {
                }
            }
            return null;
        }

        private void readText() throws IOException {
            int length = this.readLength();
            this.charsetDecoder.decode(this.byteBuffer.slice(this.byteBuffer.position(), length));
            this.skip(length);
        }

        private int readLength() throws IOException {
            long length = this.readInteger(Long.SIZE);
            if (length < 0 || length > this.byteBuffer.remaining()) {
                throw new IOException();
            }
            return (int) length;
        }

        private int readIndex(int size) throws IOException {
            long index = this.readInteger(Integer.SIZE);
            if (index < 0 || index >= size) {
                throw new IOException();
            }
            return (int) index;
        }

        private long readInteger(int width) throws IOException {
            long encoded = 0;
            for (int shift = 0; shift < width; shift += 7) {
                int nextByte = Byte.toUnsignedInt(this.byteBuffer.get());
                int remainingBits = width - shift;
                if (remainingBits < 7 && nextByte >>> remainingBits != 0) {
                    throw new IOException();
                }
                encoded |= (long) (nextByte & 0x7f) << shift;
                if ((nextByte & 0x80) == 0) {
                    return (encoded >>> 1) ^ -(encoded & 1);
                }
            }
            throw new IOException();
        }

        private void skip(int length) throws IOException {
            if (length > this.byteBuffer.remaining()) {
                throw new IOException();
            }
            this.byteBuffer.position(this.byteBuffer.position() + length);
        }

        private interface ReadFrame {

            Schema nextSchema() throws IOException;
        }

        private final class RecordFrame implements ReadFrame {

            private final Schema schema;
            private final Iterator<Schema.Field> fields;
            private final int startOffset;
            private final Integer previousOffset;

            private RecordFrame(Schema schema) throws IOException {
                this.startOffset = BinaryDatumValidator.this.byteBuffer.position();
                this.previousOffset = BinaryDatumValidator.this.recordOffsets.put(schema, this.startOffset);
                if (Objects.equals(this.previousOffset, this.startOffset)) {
                    // A recursive record cannot finish if it reenters without consuming any bytes.
                    throw new IOException();
                }
                this.schema = schema;
                this.fields = schema.getFields().iterator();
            }

            @Override
            public Schema nextSchema() {
                if (this.fields.hasNext()) {
                    return this.fields.next().schema();
                }
                if (this.startOffset == BinaryDatumValidator.this.byteBuffer.position()) {
                    // Without a discriminator or content bytes, every occurrence of this schema is zero-width.
                    BinaryDatumValidator.this.zeroWidthRecords.add(this.schema);
                }
                if (Objects.isNull(this.previousOffset)) {
                    BinaryDatumValidator.this.recordOffsets.remove(this.schema);
                } else {
                    BinaryDatumValidator.this.recordOffsets.put(this.schema, this.previousOffset);
                }
                return null;
            }
        }

        @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
        private final class CollectionFrame implements ReadFrame {

            private final Schema itemSchema;
            private final boolean hasKeys;
            private long remainingItems;
            private int enclosingLimit = -1;
            private int itemStart = -1;

            @Override
            public Schema nextSchema() throws IOException {
                ByteBuffer byteBuffer = BinaryDatumValidator.this.byteBuffer;
                if (this.itemStart == byteBuffer.position()) {
                    // A zero-width item consumes no discriminator or content. All items in this block have the same width.
                    this.remainingItems = 0;
                }
                if (this.remainingItems == 0) {
                    if (this.enclosingLimit != -1) {
                        if (byteBuffer.hasRemaining()) {
                            throw new IOException();
                        }
                        byteBuffer.limit(this.enclosingLimit);
                        this.enclosingLimit = -1;
                    }
                    this.remainingItems = BinaryDatumValidator.this.readInteger(Long.SIZE);
                    if (this.remainingItems == 0) {
                        return null;
                    }
                    if (this.remainingItems < 0) {
                        if (this.remainingItems == Long.MIN_VALUE) {
                            throw new IOException();
                        }
                        this.remainingItems = -this.remainingItems;
                        this.enclosingLimit = byteBuffer.limit();
                        int blockLength = BinaryDatumValidator.this.readLength();
                        byteBuffer.limit(byteBuffer.position() + blockLength);
                    }
                }
                this.itemStart = byteBuffer.position();
                this.remainingItems--;
                if (this.hasKeys) {
                    BinaryDatumValidator.this.readText();
                }
                return this.itemSchema;
            }
        }
    }
}
