package gregor.avro;

import gregor.dto.StoredWebhookEnvelope;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.assertj.core.api.Assertions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;

public final class WebhookEnvelopeFixtures {

    public static final String EVENT_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    public static final String EVENT_TYPE = "delivery.\uD83D\uDE80";
    public static final String TENANT_ID = "00000000-0000-0000-0000-000000000001";
    public static final String ENDPOINT_ID = "00000000-0000-0000-0000-000000000002";
    public static final Instant CREATED_AT = Instant.parse("2026-07-10T11:59:13.217Z");
    public static final Instant NEXT_ELIGIBLE_AT = Instant.parse("2026-07-10T12:00:00.456Z");
    public static final Schema PHASE_ONE_WEBHOOK_EVENT_SCHEMA = new Schema.Parser().parse(
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

    private WebhookEnvelopeFixtures() {
    }

    public static WebhookEvent currentEvent(String eventId, int replayCount, Instant nextEligibleAt) {
        return WebhookEvent
                .newBuilder()
                .setEventId(eventId)
                .setEventType(EVENT_TYPE)
                .setTenantId(TENANT_ID)
                .setEndpointId(ENDPOINT_ID)
                .setCreatedAt(CREATED_AT)
                .setPayload(ByteBuffer.wrap(new byte[]{0x7b, 0x20, 0x7d}))
                .setRetryCount(3)
                .setNextEligibleAt(nextEligibleAt)
                .setDeliveryAttemptCount(4)
                .setReplayCount(replayCount)
                .build();
    }

    public static Schema parseStoredWriterSchema(StoredWebhookEnvelope storedWebhookEnvelope) {
        return WebhookEnvelopeSchemas.parseSnapshot(storedWebhookEnvelope.writerSchemaJson());
    }

    public static Schema phaseOneSchemaWith(String writerOnlyFieldsJson) {
        return new Schema.Parser().parse(phaseOneSchemaJsonWith(writerOnlyFieldsJson));
    }

    public static String phaseOneSchemaJsonWith(String writerOnlyFieldsJson) {
        String phaseOneSchemaJson = PHASE_ONE_WEBHOOK_EVENT_SCHEMA.toString();
        int fieldsEnd = phaseOneSchemaJson.lastIndexOf(']');
        return phaseOneSchemaJson.substring(0, fieldsEnd) + "," + writerOnlyFieldsJson + phaseOneSchemaJson.substring(fieldsEnd);
    }

    public static GenericRecord phaseOneEvent(Schema writerSchema, String eventId) {
        GenericRecord genericRecord = new GenericData.Record(writerSchema);
        genericRecord.put("eventId", eventId);
        genericRecord.put("eventType", EVENT_TYPE);
        genericRecord.put("tenantId", TENANT_ID);
        genericRecord.put("endpointId", ENDPOINT_ID);
        genericRecord.put("createdAt", CREATED_AT.toEpochMilli());
        genericRecord.put("payload", ByteBuffer.wrap(new byte[]{0x7b, 0x20, 0x7d}));
        genericRecord.put("retryCount", 2);
        return genericRecord;
    }

    public static byte[] writeDatum(GenericRecord genericRecord) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
        new GenericDatumWriter<GenericRecord>(genericRecord.getSchema()).write(genericRecord, binaryEncoder);
        binaryEncoder.flush();
        return byteArrayOutputStream.toByteArray();
    }

    public static void assertPhaseOneEvent(WebhookEvent webhookEvent, String eventId) {
        assertIdentityAndPayload(webhookEvent, eventId, new byte[]{0x7b, 0x20, 0x7d});
        Assertions.assertThat(webhookEvent.getRetryCount()).isEqualTo(2);
        Assertions.assertThat(webhookEvent.getNextEligibleAt()).isNull();
        Assertions.assertThat(webhookEvent.getDeliveryAttemptCount()).isZero();
        Assertions.assertThat(webhookEvent.getReplayCount()).isZero();
    }

    static void assertIdentityAndPayload(WebhookEvent webhookEvent, String eventId, byte[] payloadBytes) {
        Assertions.assertThat(webhookEvent.getEventId()).isEqualTo(eventId);
        Assertions.assertThat(webhookEvent.getEventType()).isEqualTo(EVENT_TYPE);
        Assertions.assertThat(webhookEvent.getTenantId()).isEqualTo(TENANT_ID);
        Assertions.assertThat(webhookEvent.getEndpointId()).isEqualTo(ENDPOINT_ID);
        Assertions.assertThat(webhookEvent.getCreatedAt()).isEqualTo(CREATED_AT);
        Assertions.assertThat(webhookEvent.getPayload()).isEqualTo(ByteBuffer.wrap(payloadBytes));
    }

    public static byte[] phaseOneDatumWith(byte... suffixBytes) throws IOException {
        byte[] datumBytes = writeDatum(phaseOneEvent(PHASE_ONE_WEBHOOK_EVENT_SCHEMA, EVENT_ID));
        return suffixBytes.length == 0 ? datumBytes : appendBytes(datumBytes, suffixBytes);
    }

    static byte[] appendBytes(byte[] datumBytes, byte[] suffixBytes) {
        byte[] combinedBytes = Arrays.copyOf(datumBytes, datumBytes.length + suffixBytes.length);
        System.arraycopy(suffixBytes, 0, combinedBytes, datumBytes.length, suffixBytes.length);
        return combinedBytes;
    }
}
