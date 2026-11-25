package gregor.config.properties;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.hibernate.validator.constraints.time.DurationMax;
import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties("gregor.dead-letter")
public record DeadLetterCleanupProperties(
        @NotNull @DurationMin(millis = 1) @DurationMax(nanos = (Long.MAX_VALUE >>> 1) - 1) Duration cleanupInterval,
        @NotNull @Pattern(regexp = "[0-9]+") @DecimalMin("1") @DecimalMax("1000") String cleanupBatchSize
) {

    public int batchSize() {
        return Integer.parseInt(this.cleanupBatchSize);
    }
}
