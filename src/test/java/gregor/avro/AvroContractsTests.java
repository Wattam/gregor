package gregor.avro;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

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
                "retryCount"
        );
        Assertions.assertThat(schema.getField("createdAt").schema().getLogicalType().getName())
                .isEqualTo("timestamp-millis");
        Assertions.assertThat(schema.getField("payload").schema().getType()).isEqualTo(Schema.Type.BYTES);
        Assertions.assertThat(schema.getField("retryCount").defaultVal()).isEqualTo(0);
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
                "details"
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
}
