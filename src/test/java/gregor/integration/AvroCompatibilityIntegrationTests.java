package gregor.integration;

import gregor.avro.DeliveryAttempt;
import gregor.avro.DeliveryOutcome;
import gregor.avro.WebhookEvent;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

class AvroCompatibilityIntegrationTests {

    private static final Schema PHASE_ONE_WEBHOOK_EVENT_SCHEMA = new Schema.Parser().parse(
            """
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
            """
    );
    private static final Schema PHASE_ONE_DELIVERY_ATTEMPT_SCHEMA = new Schema.Parser().parse(
            """
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
            """
    );

    @Test
    void exposesTheExtendedContractsUnderTheExistingSchemaNames() {
        Schema webhookSchema = WebhookEvent.getClassSchema();
        Schema attemptSchema = DeliveryAttempt.getClassSchema();

        Assertions.assertThat(webhookSchema.getFullName()).isEqualTo("gregor.avro.WebhookEvent");
        Assertions.assertThat(webhookSchema.getFields()).extracting(Schema.Field::name).containsExactly(
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
        Assertions.assertThat(webhookSchema.getField("retryCount").defaultVal()).isEqualTo(0);
        Assertions.assertThat(webhookSchema.getField("nextEligibleAt").defaultVal())
                .isSameAs(JsonProperties.NULL_VALUE);
        Assertions.assertThat(webhookSchema.getField("nextEligibleAt").schema().getTypes())
                .extracting(Schema::getType)
                .containsExactly(Schema.Type.NULL, Schema.Type.LONG);
        Assertions.assertThat(
                webhookSchema.getField("nextEligibleAt").schema().getTypes().get(1).getLogicalType().getName()
        ).isEqualTo("timestamp-millis");
        Assertions.assertThat(webhookSchema.getField("deliveryAttemptCount").defaultVal()).isEqualTo(0);
        Assertions.assertThat(webhookSchema.getField("replayCount").defaultVal()).isEqualTo(0);
        Assertions.assertThat(webhookSchema.getField("schemaVersion")).isNull();

        Assertions.assertThat(attemptSchema.getFullName()).isEqualTo("gregor.avro.DeliveryAttempt");
        Assertions.assertThat(attemptSchema.getFields()).extracting(Schema.Field::name).containsExactly(
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
        Assertions.assertThat(attemptSchema.getField("retryCount").defaultVal()).isEqualTo(0);
        Assertions.assertThat(attemptSchema.getField("replayCount").defaultVal()).isEqualTo(0);
        Assertions.assertThat(attemptSchema.getField("deadLettered").defaultVal()).isEqualTo(false);
        Assertions.assertThat(attemptSchema.getField("outcome").schema().getEnumSymbols()).containsExactly(
                "SUCCEEDED",
                "RETRYABLE_FAILURE",
                "PERMANENT_FAILURE"
        );
        Assertions.assertThat(attemptSchema.getField("payload")).isNull();
        Assertions.assertThat(attemptSchema.getField("signingSecret")).isNull();
    }

    @Test
    void phaseOneWebhookEventDeserializesWithDeliveryStateDefaults() throws IOException {
        GenericRecord phaseOneEvent = new GenericData.Record(PHASE_ONE_WEBHOOK_EVENT_SCHEMA);
        phaseOneEvent.put("eventId", "00000000-0000-0000-0000-000000000010");
        phaseOneEvent.put("eventType", "compatibility.event");
        phaseOneEvent.put("tenantId", "00000000-0000-0000-0000-000000000001");
        phaseOneEvent.put("endpointId", "00000000-0000-0000-0000-000000000002");
        phaseOneEvent.put("createdAt", Instant.parse("2026-07-10T12:00:00Z").toEpochMilli());
        phaseOneEvent.put("payload", ByteBuffer.wrap("{\"compatible\":true}".getBytes(StandardCharsets.UTF_8)));
        phaseOneEvent.put("retryCount", 0);

        WebhookEvent event = read(
                PHASE_ONE_WEBHOOK_EVENT_SCHEMA,
                WebhookEvent.getClassSchema(),
                phaseOneEvent
        );

        Assertions.assertThat(event.getEventId()).isEqualTo("00000000-0000-0000-0000-000000000010");
        Assertions.assertThat(event.getRetryCount()).isZero();
        Assertions.assertThat(event.getNextEligibleAt()).isNull();
        Assertions.assertThat(event.getDeliveryAttemptCount()).isZero();
        Assertions.assertThat(event.getReplayCount()).isZero();
    }

    @Test
    void phaseOneDeliveryAttemptDeserializesWithAuditDefaults() throws IOException {
        GenericRecord phaseOneAttempt = new GenericData.Record(PHASE_ONE_DELIVERY_ATTEMPT_SCHEMA);
        phaseOneAttempt.put("attemptId", "00000000-0000-0000-0000-000000000020");
        phaseOneAttempt.put("eventId", "00000000-0000-0000-0000-000000000010");
        phaseOneAttempt.put("tenantId", "00000000-0000-0000-0000-000000000001");
        phaseOneAttempt.put("endpointId", "00000000-0000-0000-0000-000000000002");
        phaseOneAttempt.put("eventType", "compatibility.attempt");
        phaseOneAttempt.put("attemptNumber", 2);
        Schema outcomeSchema = PHASE_ONE_DELIVERY_ATTEMPT_SCHEMA.getField("outcome").schema();
        phaseOneAttempt.put("outcome", new GenericData.EnumSymbol(outcomeSchema, "RETRYABLE_FAILURE"));
        phaseOneAttempt.put("httpStatus", 503);
        phaseOneAttempt.put("latencyMs", 25L);
        phaseOneAttempt.put("attemptedAt", Instant.parse("2026-07-10T12:00:01Z").toEpochMilli());
        phaseOneAttempt.put("details", ByteBuffer.wrap("{\"type\":\"HTTP_FAILURE\"}".getBytes(StandardCharsets.UTF_8)));

        DeliveryAttempt attempt = read(
                PHASE_ONE_DELIVERY_ATTEMPT_SCHEMA,
                DeliveryAttempt.getClassSchema(),
                phaseOneAttempt
        );

        Assertions.assertThat(attempt.getAttemptId()).isEqualTo("00000000-0000-0000-0000-000000000020");
        Assertions.assertThat(attempt.getOutcome()).isEqualTo(DeliveryOutcome.RETRYABLE_FAILURE);
        Assertions.assertThat(attempt.getRetryCount()).isZero();
        Assertions.assertThat(attempt.getReplayCount()).isZero();
        Assertions.assertThat(attempt.getDeadLettered()).isFalse();
    }

    private static <T> T read(
            Schema writerSchema,
            Schema readerSchema,
            GenericRecord record
    ) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(writerSchema);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
        writer.write(record, encoder);
        encoder.flush();

        SpecificDatumReader<T> reader = new SpecificDatumReader<>(writerSchema, readerSchema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(output.toByteArray(), null);
        return reader.read(null, decoder);
    }
}
