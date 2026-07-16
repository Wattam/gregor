package gregor.entity;

import gregor.repository.DeliveryAttemptRepository;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import org.assertj.core.api.Assertions;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import org.junit.jupiter.api.Test;
import org.springframework.core.ResolvableType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

class DeliveryAttemptPersistenceMappingTests {

    @Test
    void mapsTheDefaultTableAndNamedEventAttemptUniqueness() throws ReflectiveOperationException {
        Table table = DeliveryAttempt.class.getAnnotation(Table.class);

        Assertions.assertThat(DeliveryAttempt.class.isAnnotationPresent(Entity.class)).isTrue();
        Assertions.assertThat(table).isNotNull();
        Assertions.assertThat(table.name()).isEmpty();
        Assertions.assertThat(table.uniqueConstraints())
                .extracting(UniqueConstraint::name)
                .containsExactly("uk_delivery_attempt_event_number");
        Assertions.assertThat(table.uniqueConstraints()[0].columnNames())
                .containsExactly("event_id", "attempt_number");

        Field attemptId = DeliveryAttempt.class.getDeclaredField("attemptId");
        Assertions.assertThat(attemptId.getType()).isEqualTo(UUID.class);
        Assertions.assertThat(attemptId.isAnnotationPresent(Id.class)).isTrue();
        Assertions.assertThat(attemptId.isAnnotationPresent(Column.class)).isFalse();
        Field eventId = DeliveryAttempt.class.getDeclaredField("eventId");
        Field tenantId = DeliveryAttempt.class.getDeclaredField("tenantId");
        Field endpointId = DeliveryAttempt.class.getDeclaredField("endpointId");
        Assertions.assertThat(eventId.getType()).isEqualTo(UUID.class);
        Assertions.assertThat(tenantId.getType()).isEqualTo(UUID.class);
        Assertions.assertThat(endpointId.getType()).isEqualTo(UUID.class);
        assertColumnNotNullable(eventId);
        assertColumnNotNullable(tenantId);
        assertColumnNotNullable(endpointId);
        Field eventType = DeliveryAttempt.class.getDeclaredField("eventType");
        Assertions.assertThat(eventType.getType()).isEqualTo(String.class);
        assertColumnNotNullable(eventType);
        Assertions.assertThat(eventType.getAnnotation(Column.class).columnDefinition()).isEqualToIgnoringCase("TEXT");
        Field attemptNumber = DeliveryAttempt.class.getDeclaredField("attemptNumber");
        Assertions.assertThat(attemptNumber.getType()).isEqualTo(int.class);
        assertColumnNotNullable(attemptNumber);
        Field outcome = DeliveryAttempt.class.getDeclaredField("outcome");
        Assertions.assertThat(outcome.getType()).isEqualTo(gregor.dto.DeliveryOutcome.class);
        Assertions.assertThat(outcome.getAnnotation(Enumerated.class).value()).isEqualTo(EnumType.STRING);
        Assertions.assertThat(outcome.getAnnotation(Column.class).length()).isEqualTo(32);
        Assertions.assertThat(outcome.getAnnotation(Column.class).nullable()).isFalse();
        Field httpStatus = DeliveryAttempt.class.getDeclaredField("httpStatus");
        Assertions.assertThat(httpStatus.getType()).isEqualTo(Integer.class);
        Assertions.assertThat(httpStatus.isAnnotationPresent(Column.class)).isFalse();
        Field latencyMs = DeliveryAttempt.class.getDeclaredField("latencyMs");
        Assertions.assertThat(latencyMs.getType()).isEqualTo(long.class);
        assertColumnNotNullable(latencyMs);
        Field attemptedAt = DeliveryAttempt.class.getDeclaredField("attemptedAt");
        Assertions.assertThat(attemptedAt.getType()).isEqualTo(Instant.class);
        Assertions.assertThat(attemptedAt.getAnnotation(Column.class).nullable()).isFalse();
        Assertions.assertThat(attemptedAt.getAnnotation(Column.class).columnDefinition())
                .isEqualToIgnoringCase("TIMESTAMPTZ");
        Field details = DeliveryAttempt.class.getDeclaredField("details");
        Assertions.assertThat(details.getType()).isEqualTo(Map.class);
        Assertions.assertThat(details.getGenericType().getTypeName())
                .isEqualTo("java.util.Map<java.lang.String, java.lang.String>");
        Assertions.assertThat(details.getAnnotation(JdbcTypeCode.class).value()).isEqualTo(SqlTypes.JSON);
        Assertions.assertThat(details.getAnnotation(Column.class).nullable()).isFalse();
        Assertions.assertThat(details.getAnnotation(Column.class).columnDefinition()).isEqualToIgnoringCase("JSONB");
        Field retryCount = DeliveryAttempt.class.getDeclaredField("retryCount");
        Assertions.assertThat(retryCount.getType()).isEqualTo(int.class);
        assertColumnNotNullable(retryCount);
        Field replayCount = DeliveryAttempt.class.getDeclaredField("replayCount");
        Assertions.assertThat(replayCount.getType()).isEqualTo(int.class);
        assertColumnNotNullable(replayCount);
        Field deadLettered = DeliveryAttempt.class.getDeclaredField("deadLettered");
        Assertions.assertThat(deadLettered.getType()).isEqualTo(boolean.class);
        assertColumnNotNullable(deadLettered);
        Assertions.assertThat(Arrays.stream(DeliveryAttempt.class.getDeclaredFields()).map(Field::getName))
                .doesNotContain("payload", "signingSecret");
    }

    @Test
    void repositoryDeclaresTheTransactionalIdempotentPostgresqlInsert() {
        ResolvableType repositoryType = ResolvableType.forClass(DeliveryAttemptRepository.class)
                .as(JpaRepository.class);
        Assertions.assertThat(repositoryType.getGeneric(0).resolve()).isEqualTo(DeliveryAttempt.class);
        Assertions.assertThat(repositoryType.getGeneric(1).resolve()).isEqualTo(UUID.class);
        Method[] queryMethods = Arrays.stream(DeliveryAttemptRepository.class.getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(Query.class))
                .toArray(Method[]::new);
        Assertions.assertThat(queryMethods).hasSize(1);
        Method insert = queryMethods[0];
        Query query = insert.getAnnotation(Query.class);
        String normalizedSql = query.value().replaceAll("\\s+", " ").trim().toUpperCase();

        Assertions.assertThat(insert.isAnnotationPresent(Modifying.class)).isTrue();
        Assertions.assertThat(insert.isAnnotationPresent(Transactional.class)).isTrue();
        Assertions.assertThat(query.nativeQuery()).isTrue();
        Assertions.assertThat(normalizedSql)
                .contains("INSERT INTO DELIVERY_ATTEMPT")
                .contains("ATTEMPTED_AT, DETAILS, RETRY_COUNT, REPLAY_COUNT, DEAD_LETTERED")
                .contains("CAST(:DETAILS AS JSONB), :RETRYCOUNT, :REPLAYCOUNT, :DEADLETTERED")
                .contains("ON CONFLICT (EVENT_ID, ATTEMPT_NUMBER) DO NOTHING")
                .doesNotContain("PAYLOAD", "SIGNING_SECRET");
        Set<String> parameterNames = Arrays.stream(insert.getParameters())
                .map(Parameter::getAnnotations)
                .flatMap(Arrays::stream)
                .filter(Param.class::isInstance)
                .map(Param.class::cast)
                .map(Param::value)
                .collect(Collectors.toSet());
        Assertions.assertThat(insert.getReturnType()).isEqualTo(int.class);
        Assertions.assertThat(insert.getParameterTypes()).containsExactly(
                UUID.class,
                UUID.class,
                UUID.class,
                UUID.class,
                String.class,
                int.class,
                String.class,
                Integer.class,
                long.class,
                Instant.class,
                String.class,
                int.class,
                int.class,
                boolean.class
        );
        Assertions.assertThat(parameterNames).containsExactlyInAnyOrder(
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
    }

    private static void assertColumnNotNullable(Field field) {
        Column column = field.getAnnotation(Column.class);
        Assertions.assertThat(column).isNotNull();
        Assertions.assertThat(column.nullable()).isFalse();
    }
}
