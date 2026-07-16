package gregor.avro;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

class AvroContractsTests {

    @Test
    void webhookEventExposesTheContractWithoutSettersOrSecrets() throws ReflectiveOperationException {
        Schema schema = WebhookEvent.getClassSchema();

        Assertions.assertThat(schema.getNamespace()).isEqualTo("gregor.avro");
        Assertions.assertThat(schema.getFields()).extracting(Schema.Field::name).containsExactly(
                "eventId",
                "eventType",
                "tenantId",
                "endpointId",
                "createdAt",
                "payload",
                "retryCount",
                "nextEligibleAt",
                "deliveryAttemptCount",
                "replayCount"
        );
        Assertions.assertThat(schema.getField("createdAt").schema().getLogicalType().getName())
                .isEqualTo("timestamp-millis");
        Assertions.assertThat(schema.getField("payload").schema().getType()).isEqualTo(Schema.Type.BYTES);
        Assertions.assertThat(schema.getField("retryCount").defaultVal()).isEqualTo(0);
        Assertions.assertThat(schema.getField("nextEligibleAt").schema().getTypes())
                .extracting(Schema::getType)
                .containsExactly(Schema.Type.NULL, Schema.Type.LONG);
        Assertions.assertThat(schema.getField("nextEligibleAt").schema().getTypes().get(1).getLogicalType().getName())
                .isEqualTo("timestamp-millis");
        Assertions.assertThat(schema.getField("nextEligibleAt").defaultVal()).isSameAs(JsonProperties.NULL_VALUE);
        Assertions.assertThat(schema.getField("deliveryAttemptCount").defaultVal()).isEqualTo(0);
        Assertions.assertThat(schema.getField("replayCount").defaultVal()).isEqualTo(0);
        Assertions.assertThat(schema.getField("signingSecret")).isNull();
        Assertions.assertThat(WebhookEvent.class.getMethod("getEventId").getReturnType()).isEqualTo(String.class);
        Assertions.assertThat(WebhookEvent.class.getMethod("getCreatedAt").getReturnType()).isEqualTo(Instant.class);
        Assertions.assertThat(WebhookEvent.class.getMethod("getPayload").getReturnType()).isEqualTo(ByteBuffer.class);
        Assertions.assertThat(Arrays.stream(WebhookEvent.class.getDeclaredMethods()))
                .noneMatch(method -> method.getName().startsWith("set"));
    }

    @Test
    void deliveryAttemptExposesTypedDimensionsNullableStatusAndInlineOutcome() throws ReflectiveOperationException {
        Schema schema = DeliveryAttempt.getClassSchema();

        Assertions.assertThat(schema.getNamespace()).isEqualTo("gregor.avro");
        Assertions.assertThat(schema.getFields()).extracting(Schema.Field::name).containsExactly(
                "attemptId",
                "eventId",
                "tenantId",
                "endpointId",
                "eventType",
                "attemptNumber",
                "outcome",
                "httpStatus",
                "latencyMs",
                "attemptedAt",
                "details",
                "retryCount",
                "replayCount",
                "deadLettered"
        );
        Assertions.assertThat(schema.getField("outcome").schema().getEnumSymbols()).containsExactly(
                "SUCCEEDED",
                "RETRYABLE_FAILURE",
                "PERMANENT_FAILURE"
        );
        Assertions.assertThat(schema.getField("httpStatus").schema().getTypes())
                .extracting(Schema::getType)
                .containsExactly(Schema.Type.NULL, Schema.Type.INT);
        Assertions.assertThat(schema.getField("httpStatus").hasDefaultValue()).isTrue();
        Assertions.assertThat(schema.getField("httpStatus").defaultVal()).isSameAs(JsonProperties.NULL_VALUE);
        Assertions.assertThat(schema.getField("attemptedAt").schema().getLogicalType().getName())
                .isEqualTo("timestamp-millis");
        Assertions.assertThat(schema.getField("details").schema().getType()).isEqualTo(Schema.Type.BYTES);
        Assertions.assertThat(schema.getField("retryCount").defaultVal()).isEqualTo(0);
        Assertions.assertThat(schema.getField("replayCount").defaultVal()).isEqualTo(0);
        Assertions.assertThat(schema.getField("deadLettered").defaultVal()).isEqualTo(false);
        Assertions.assertThat(schema.getField("signingSecret")).isNull();
        Assertions.assertThat(schema.getField("payload")).isNull();
        Assertions.assertThat(DeliveryAttempt.class.getMethod("getAttemptId").getReturnType())
                .isEqualTo(String.class);
        Assertions.assertThat(DeliveryAttempt.class.getMethod("getAttemptedAt").getReturnType())
                .isEqualTo(Instant.class);
        Assertions.assertThat(DeliveryAttempt.class.getMethod("getDetails").getReturnType())
                .isEqualTo(ByteBuffer.class);
        Assertions.assertThat(DeliveryOutcome.getClassSchema().getEnumSymbols())
                .isEqualTo(List.of("SUCCEEDED", "RETRYABLE_FAILURE", "PERMANENT_FAILURE"));
        Assertions.assertThat(Arrays.stream(DeliveryAttempt.class.getDeclaredMethods()))
                .noneMatch(method -> method.getName().startsWith("set"));
    }

    @Test
    void webhookEventResolvesDeliveryStateDefaultsFromPhaseOneWriterData() throws Exception {
        Schema writerSchema = new Schema.Parser().parse("""
                                                        {
                                                            "type": "record",
                                                            "name": "WebhookEvent",
                                                            "namespace": "gregor.avro",
                                                            "fields": [
                                                                {"name": "eventId", "type": "string"},
                                                                {"name": "eventType", "type": "string"},
                                                                {"name": "tenantId", "type": "string"},
                                                                {"name": "endpointId", "type": "string"},
                                                                {
                                                                    "name": "createdAt",
                                                                    "type": {"type": "long", "logicalType": "timestamp-millis"}
                                                                },
                                                                {"name": "payload", "type": "bytes"},
                                                                {"name": "retryCount", "type": "int", "default": 0}
                                                            ]
                                                        }
                                                        """);
        GenericRecord writerRecord = new GenericData.Record(writerSchema);
        writerRecord.put("eventId", "event-1");
        writerRecord.put("eventType", "order.created");
        writerRecord.put("tenantId", "tenant-1");
        writerRecord.put("endpointId", "endpoint-1");
        writerRecord.put("createdAt", 0L);
        writerRecord.put("payload", ByteBuffer.wrap(new byte[]{1}));
        writerRecord.put("retryCount", 2);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);
        new GenericDatumWriter<GenericRecord>(writerSchema).write(writerRecord, encoder);
        encoder.flush();
        SpecificDatumReader<WebhookEvent> reader =
                new SpecificDatumReader<>(writerSchema, WebhookEvent.getClassSchema());

        WebhookEvent resolved = reader.read(
                null,
                DecoderFactory.get().binaryDecoder(output.toByteArray(), null)
        );

        Assertions.assertThat(resolved.getRetryCount()).isEqualTo(2);
        Assertions.assertThat(resolved.getNextEligibleAt()).isNull();
        Assertions.assertThat(resolved.getDeliveryAttemptCount()).isZero();
        Assertions.assertThat(resolved.getReplayCount()).isZero();
    }

    @Test
    void deliveryAttemptResolvesAuditDefaultsFromPhaseOneWriterData() throws Exception {
        Schema writerSchema = new Schema.Parser().parse("""
                                                        {
                                                            "type": "record",
                                                            "name": "DeliveryAttempt",
                                                            "namespace": "gregor.avro",
                                                            "fields": [
                                                                {"name": "attemptId", "type": "string"},
                                                                {"name": "eventId", "type": "string"},
                                                                {"name": "tenantId", "type": "string"},
                                                                {"name": "endpointId", "type": "string"},
                                                                {"name": "eventType", "type": "string"},
                                                                {"name": "attemptNumber", "type": "int"},
                                                                {
                                                                    "name": "outcome",
                                                                    "type": {
                                                                        "type": "enum",
                                                                        "name": "DeliveryOutcome",
                                                                        "symbols": ["SUCCEEDED", "RETRYABLE_FAILURE", "PERMANENT_FAILURE"]
                                                                    }
                                                                },
                                                                {"name": "httpStatus", "type": ["null", "int"], "default": null},
                                                                {"name": "latencyMs", "type": "long"},
                                                                {
                                                                    "name": "attemptedAt",
                                                                    "type": {"type": "long", "logicalType": "timestamp-millis"}
                                                                },
                                                                {"name": "details", "type": "bytes"}
                                                            ]
                                                        }
                                                        """);
        GenericRecord writerRecord = new GenericData.Record(writerSchema);
        writerRecord.put("attemptId", "attempt-1");
        writerRecord.put("eventId", "event-1");
        writerRecord.put("tenantId", "tenant-1");
        writerRecord.put("endpointId", "endpoint-1");
        writerRecord.put("eventType", "order.created");
        writerRecord.put("attemptNumber", 1);
        writerRecord.put(
                "outcome",
                new GenericData.EnumSymbol(writerSchema.getField("outcome").schema(), "RETRYABLE_FAILURE")
        );
        writerRecord.put("httpStatus", null);
        writerRecord.put("latencyMs", 5L);
        writerRecord.put("attemptedAt", 0L);
        writerRecord.put("details", ByteBuffer.wrap(new byte[]{1}));
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);
        new GenericDatumWriter<GenericRecord>(writerSchema).write(writerRecord, encoder);
        encoder.flush();
        SpecificDatumReader<DeliveryAttempt> reader =
                new SpecificDatumReader<>(writerSchema, DeliveryAttempt.getClassSchema());

        DeliveryAttempt resolved = reader.read(
                null,
                DecoderFactory.get().binaryDecoder(output.toByteArray(), null)
        );

        Assertions.assertThat(resolved.getOutcome()).isEqualTo(DeliveryOutcome.RETRYABLE_FAILURE);
        Assertions.assertThat(resolved.getRetryCount()).isZero();
        Assertions.assertThat(resolved.getReplayCount()).isZero();
        Assertions.assertThat(resolved.getDeadLettered()).isFalse();
    }
}
