package gregor.service;

import gregor.config.properties.KafkaTopicProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

@Component
@RequiredArgsConstructor
public class DeadLetterRetention {

    // PostgreSQL's first finite timestamp is the start of Julian day zero.
    private static final Instant POSTGRES_MIN_TIMESTAMP = Instant.parse("-4713-11-24T00:00:00Z");
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("y-MM-dd HH:mm:ss.SSSxxx G", Locale.US).withZone(ZoneOffset.UTC);

    private final Clock clock;
    private final KafkaTopicProperties kafkaTopicProperties;

    public String cutoff() {
        Instant now = this.clock.instant();
        Duration retention = this.kafkaTopicProperties.deadLetterRetention();
        // Check before subtracting: a valid retention can place the cutoff outside either timestamp type's range.
        if (retention.compareTo(Duration.between(POSTGRES_MIN_TIMESTAMP, now)) > 0) {
            return "-infinity";
        }
        // Let PostgreSQL parse the cutoff directly to avoid JDBC's historical date conversions.
        return DATE_TIME_FORMATTER.format(now.minus(retention).truncatedTo(ChronoUnit.MILLIS));
    }
}
