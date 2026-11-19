package gregor.dto;

import java.time.Duration;

public record RetryTier(String topic, Duration delay) {}
