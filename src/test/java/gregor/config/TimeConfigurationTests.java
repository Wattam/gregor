package gregor.config;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

class TimeConfigurationTests {

    @Test
    void exposesASystemUtcClock() {
        TimeConfiguration timeConfiguration = new TimeConfiguration();
        Instant before = Instant.now();

        Clock clock = timeConfiguration.clock();
        Instant clockInstant = clock.instant();
        Instant after = Instant.now();

        Assertions.assertThat(clock.getZone()).isEqualTo(ZoneOffset.UTC);
        Assertions.assertThat(clockInstant).isBetween(before, after);
    }
}
