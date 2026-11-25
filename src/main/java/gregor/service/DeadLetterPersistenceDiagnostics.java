package gregor.service;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Synchronized;
import org.apache.commons.lang3.Strings;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class DeadLetterPersistenceDiagnostics {

    private static final Set<String> DATABASE_LOGGER_PREFIXES = Set.of("com.zaxxer.hikari.", "org.hibernate.", "org.springframework.jdbc.", "org.springframework.orm.jpa.", "org.springframework.transaction.");
    private static final ThreadLocal<Boolean> DATABASE_LOGGING_SUPPRESSED = new ThreadLocal<>();
    @SuppressWarnings("unused")
    private static final Object FILTER_INSTALLATION_LOCK = new Object();

    static void withoutRawDatabaseLogging(Runnable runnable) {
        withoutRawDatabaseLogging(() -> {
            runnable.run();
            return null;
        });
    }

    static <T> T withoutRawDatabaseLogging(Supplier<T> supplier) {
        if (LoggerFactory.getILoggerFactory() instanceof LoggerContext loggerContext) {
            installFilter(loggerContext);
        }
        Boolean previouslySuppressed = DATABASE_LOGGING_SUPPRESSED.get();
        DATABASE_LOGGING_SUPPRESSED.set(true);
        try {
            return supplier.get();
        } finally {
            if (Objects.isNull(previouslySuppressed)) {
                DATABASE_LOGGING_SUPPRESSED.remove();
            } else {
                DATABASE_LOGGING_SUPPRESSED.set(previouslySuppressed);
            }
        }
    }

    @Synchronized("FILTER_INSTALLATION_LOCK")
    private static void installFilter(LoggerContext loggerContext) {
        if (loggerContext.getTurboFilterList().stream().noneMatch(DatabaseLoggingTurboFilter.class::isInstance)) {
            DatabaseLoggingTurboFilter databaseLoggingTurboFilter = new DatabaseLoggingTurboFilter();
            databaseLoggingTurboFilter.setContext(loggerContext);
            databaseLoggingTurboFilter.start();
            loggerContext.getTurboFilterList().addFirst(databaseLoggingTurboFilter);
        }
    }

    private static final class DatabaseLoggingTurboFilter extends TurboFilter {

        @Override
        public FilterReply decide(Marker marker, Logger logger, Level level, String format, Object[] arguments, Throwable throwable) {
            if (Objects.equals(Boolean.TRUE, DATABASE_LOGGING_SUPPRESSED.get()) && DATABASE_LOGGER_PREFIXES.stream().anyMatch(prefix -> Strings.CS.startsWith(logger.getName(), prefix))) {
                return FilterReply.DENY;
            }
            return FilterReply.NEUTRAL;
        }
    }
}
