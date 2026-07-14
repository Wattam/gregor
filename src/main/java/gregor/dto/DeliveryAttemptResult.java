package gregor.dto;

import java.time.Instant;
import java.util.Map;

public record DeliveryAttemptResult(
        DeliveryOutcome outcome,
        Integer httpStatus,
        long latencyMs,
        Instant attemptedAt,
        Map<String, String> details
) {}
