package gregor.config.properties;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties("camel.component.kafka")
public record KafkaConsumerProperties(@NotNull @Min(MINIMUM_MAX_POLL_INTERVAL_MS) Integer maxPollIntervalMs) {

    public static final Duration POLL_INTERVAL_SAFETY_MARGIN = Duration.ofMinutes(5);
    public static final int MINIMUM_MAX_POLL_INTERVAL_MS = 3_900_001;

    public Duration maximumPermittedWait() {
        return Duration.ofMillis(this.maxPollIntervalMs).minus(POLL_INTERVAL_SAFETY_MARGIN);
    }
}
