package gregor.config;

import gregor.config.properties.DeadLetterCleanupProperties;
import gregor.service.DeadLetterOccurrenceCleanupService;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.FixedDelayTask;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import java.time.Duration;

@EnableScheduling
@RequiredArgsConstructor
@Configuration(proxyBeanMethods = false)
public class DeadLetterCleanupConfiguration implements SchedulingConfigurer {

    private final DeadLetterCleanupProperties deadLetterCleanupProperties;
    private final DeadLetterOccurrenceCleanupService deadLetterOccurrenceCleanupService;
    private final TaskScheduler taskScheduler;

    @Override
    public void configureTasks(ScheduledTaskRegistrar scheduledTaskRegistrar) {
        scheduledTaskRegistrar.setTaskScheduler(this.taskScheduler);
        Duration cleanupInterval = this.deadLetterCleanupProperties.cleanupInterval();
        scheduledTaskRegistrar.addFixedDelayTask(new FixedDelayTask(this.deadLetterOccurrenceCleanupService::runCleanup, cleanupInterval, cleanupInterval));
    }
}
