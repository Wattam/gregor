package gregor.entity;

import gregor.dto.DeliveryOutcome;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Getter
@Setter
@Entity
@NoArgsConstructor
@Table(uniqueConstraints = @UniqueConstraint(name = "uk_delivery_attempt_event_number", columnNames = {"event_id", "attempt_number"}))
public class DeliveryAttempt {

    @Id
    private UUID attemptId;

    @Column(nullable = false)
    private UUID eventId;

    @Column(nullable = false)
    private UUID tenantId;

    @Column(nullable = false)
    private UUID endpointId;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String eventType;

    @Column(nullable = false)
    private int attemptNumber;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private DeliveryOutcome outcome;

    private Integer httpStatus;

    @Column(nullable = false)
    private long latencyMs;

    @Column(nullable = false, columnDefinition = "TIMESTAMPTZ")
    private Instant attemptedAt;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "JSONB")
    private Map<String, String> details;

    @Column(nullable = false)
    private int retryCount;

    @Column(nullable = false)
    private int replayCount;

    @Column(nullable = false)
    private boolean deadLettered;
}
