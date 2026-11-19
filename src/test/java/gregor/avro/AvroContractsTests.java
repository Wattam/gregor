package gregor.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.ClassSecurityValidator;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.GenericApplicationContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

class AvroContractsTests {

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
                    {"name": "createdAt", "type": {"type": "long", "logicalType": "timestamp-millis"}},
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
                    {"name": "attemptedAt", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                    {"name": "details", "type": "bytes"}
                ]
            }
            """
    );

    private static ClassSecurityValidator.ClassSecurityPredicate previousClassSecurityPredicate;

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

    @Test
    void rejectsClassesOutsideTheAllowlist() {
        Assertions.assertThatThrownBy(() -> ClassSecurityValidator.validate(AvroContractsTests.class)).isInstanceOf(SecurityException.class);
    }

    @Test
    void excludesSensitiveFieldsFromTheWireSchemas() {
        Assertions.assertThat(WebhookEvent.getClassSchema().getField("signingSecret")).isNull();
        Assertions.assertThat(DeliveryAttempt.getClassSchema().getFields()).extracting(Schema.Field::name).doesNotContain("payload", "signingSecret");
    }

    @Test
    void phaseOneWebhookEventDeserializesWithDeliveryStateDefaults() throws IOException {
        GenericRecord phaseOneWebhookEventGenericRecord = new GenericData.Record(PHASE_ONE_WEBHOOK_EVENT_SCHEMA);
        phaseOneWebhookEventGenericRecord.put("eventId", "00000000-0000-0000-0000-000000000010");
        phaseOneWebhookEventGenericRecord.put("eventType", "compatibility.event");
        phaseOneWebhookEventGenericRecord.put("tenantId", "00000000-0000-0000-0000-000000000001");
        phaseOneWebhookEventGenericRecord.put("endpointId", "00000000-0000-0000-0000-000000000002");
        phaseOneWebhookEventGenericRecord.put("createdAt", Instant.parse("2026-07-10T12:00:00Z").toEpochMilli());
        phaseOneWebhookEventGenericRecord.put("payload", ByteBuffer.wrap("{\"compatible\":true}".getBytes(StandardCharsets.UTF_8)));
        phaseOneWebhookEventGenericRecord.put("retryCount", 2);

        WebhookEvent webhookEvent = read(PHASE_ONE_WEBHOOK_EVENT_SCHEMA, WebhookEvent.getClassSchema(), phaseOneWebhookEventGenericRecord);

        Assertions.assertThat(webhookEvent.getEventId()).isEqualTo("00000000-0000-0000-0000-000000000010");
        Assertions.assertThat(webhookEvent.getCreatedAt()).isEqualTo(Instant.parse("2026-07-10T12:00:00Z"));
        Assertions.assertThat(webhookEvent.getPayload()).isEqualTo(ByteBuffer.wrap("{\"compatible\":true}".getBytes(StandardCharsets.UTF_8)));
        Assertions.assertThat(webhookEvent.getRetryCount()).isEqualTo(2);
        Assertions.assertThat(webhookEvent.getNextEligibleAt()).isNull();
        Assertions.assertThat(webhookEvent.getDeliveryAttemptCount()).isZero();
        Assertions.assertThat(webhookEvent.getReplayCount()).isZero();
    }

    @Test
    void phaseOneDeliveryAttemptDeserializesWithAuditDefaults() throws IOException {
        GenericRecord phaseOneDeliveryAttemptGenericRecord = new GenericData.Record(PHASE_ONE_DELIVERY_ATTEMPT_SCHEMA);
        phaseOneDeliveryAttemptGenericRecord.put("attemptId", "00000000-0000-0000-0000-000000000020");
        phaseOneDeliveryAttemptGenericRecord.put("eventId", "00000000-0000-0000-0000-000000000010");
        phaseOneDeliveryAttemptGenericRecord.put("tenantId", "00000000-0000-0000-0000-000000000001");
        phaseOneDeliveryAttemptGenericRecord.put("endpointId", "00000000-0000-0000-0000-000000000002");
        phaseOneDeliveryAttemptGenericRecord.put("eventType", "compatibility.attempt");
        phaseOneDeliveryAttemptGenericRecord.put("attemptNumber", 2);
        Schema outcomeSchema = PHASE_ONE_DELIVERY_ATTEMPT_SCHEMA.getField("outcome").schema();
        phaseOneDeliveryAttemptGenericRecord.put("outcome", new GenericData.EnumSymbol(outcomeSchema, "RETRYABLE_FAILURE"));
        phaseOneDeliveryAttemptGenericRecord.put("httpStatus", null);
        phaseOneDeliveryAttemptGenericRecord.put("latencyMs", 25L);
        phaseOneDeliveryAttemptGenericRecord.put("attemptedAt", Instant.parse("2026-07-10T12:00:01Z").toEpochMilli());
        phaseOneDeliveryAttemptGenericRecord.put("details", ByteBuffer.wrap("{\"type\":\"HTTP_FAILURE\"}".getBytes(StandardCharsets.UTF_8)));

        DeliveryAttempt deliveryAttempt = read(PHASE_ONE_DELIVERY_ATTEMPT_SCHEMA, DeliveryAttempt.getClassSchema(), phaseOneDeliveryAttemptGenericRecord);

        Assertions.assertThat(deliveryAttempt.getAttemptId()).isEqualTo("00000000-0000-0000-0000-000000000020");
        Assertions.assertThat(deliveryAttempt.getOutcome()).isEqualTo(DeliveryOutcome.RETRYABLE_FAILURE);
        Assertions.assertThat(deliveryAttempt.getHttpStatus()).isNull();
        Assertions.assertThat(deliveryAttempt.getAttemptedAt()).isEqualTo(Instant.parse("2026-07-10T12:00:01Z"));
        Assertions.assertThat(deliveryAttempt.getRetryCount()).isZero();
        Assertions.assertThat(deliveryAttempt.getReplayCount()).isZero();
        Assertions.assertThat(deliveryAttempt.getDeadLettered()).isFalse();
    }

    private static <T> T read(Schema writerSchema, Schema readerSchema, GenericRecord genericRecord) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> genericDatumWriter = new GenericDatumWriter<>(writerSchema);
        BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
        genericDatumWriter.write(genericRecord, binaryEncoder);
        binaryEncoder.flush();

        SpecificDatumReader<T> specificDatumReader = new SpecificDatumReader<>(writerSchema, readerSchema);
        BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(byteArrayOutputStream.toByteArray(), null);
        return specificDatumReader.read(null, binaryDecoder);
    }
}
