package gregor.repository;

import gregor.entity.DeliveryAttempt;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

public interface DeliveryAttemptRepository extends JpaRepository<DeliveryAttempt, UUID> {

    @Modifying
    @Transactional
    @Query(
            value = """
                    INSERT INTO delivery_attempt (
                        attempt_id, event_id, tenant_id, endpoint_id, event_type,
                        attempt_number, outcome, http_status, latency_ms, attempted_at, details
                    ) VALUES (
                        :attemptId, :eventId, :tenantId, :endpointId, :eventType,
                        :attemptNumber, :outcome, :httpStatus, :latencyMs, :attemptedAt,
                        CAST(:details AS jsonb)
                    )
                    ON CONFLICT (event_id, attempt_number) DO NOTHING
                    """,
            nativeQuery = true
    )
    int insertIfAbsent(
            @Param("attemptId") UUID attemptId,
            @Param("eventId") UUID eventId,
            @Param("tenantId") UUID tenantId,
            @Param("endpointId") UUID endpointId,
            @Param("eventType") String eventType,
            @Param("attemptNumber") int attemptNumber,
            @Param("outcome") String outcome,
            @Param("httpStatus") Integer httpStatus,
            @Param("latencyMs") long latencyMs,
            @Param("attemptedAt") Instant attemptedAt,
            @Param("details") String details
    );
}
