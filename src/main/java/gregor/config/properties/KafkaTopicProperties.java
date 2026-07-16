package gregor.config.properties;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties("gregor.kafka.topics")
public record KafkaTopicProperties(
        @Min(1) int defaultPartitions,
        @Min(1) @Max(Short.MAX_VALUE) int replicationFactor,
        @NotNull @DurationMin(hours = 1, inclusive = false) Duration retryRetention,
        @NotNull @DurationMin(inclusive = false) Duration deadLetterRetention
) {}
