package gregor.config.properties;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("gregor.kafka.topics")
public record KafkaTopicProperties(
        @Min(1) int defaultPartitions,
        @Min(1) @Max(Short.MAX_VALUE) int replicationFactor
) {}
