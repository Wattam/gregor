package gregor.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Component
public class DeadLetterCleanupMetrics {

    private static final long NOT_SAMPLED = -1L;
    private static final String RUNS_METER_NAME = "gregor.deadletter.cleanup.runs";

    private final AtomicLong retainedCount = new AtomicLong(NOT_SAMPLED);
    private final Counter deletedCounter;
    private final Counter successfulRunCounter;
    private final Counter failedRunCounter;

    public DeadLetterCleanupMetrics(MeterRegistry meterRegistry) {
        Gauge.builder(
                "gregor.deadletter.occurrences.retained",
                this.retainedCount,
                retainedCount -> {
                    long sample = retainedCount.get();
                    return sample == NOT_SAMPLED ? Double.NaN : sample;
                }
        ).register(meterRegistry);
        this.deletedCounter = meterRegistry.counter("gregor.deadletter.cleanup.deleted");
        this.successfulRunCounter = meterRegistry.counter(RUNS_METER_NAME, "outcome", "success");
        this.failedRunCounter = meterRegistry.counter(RUNS_METER_NAME, "outcome", "failure");
    }

    void recordRetainedCount(long retainedCount) {
        this.retainedCount.set(retainedCount);
    }

    void recordSuccessfulBatch(int deletedCount) {
        if (deletedCount > 0) {
            this.deletedCounter.increment(deletedCount);
        }
        this.successfulRunCounter.increment();
    }

    void recordFailedBatch() {
        this.failedRunCounter.increment();
    }
}
