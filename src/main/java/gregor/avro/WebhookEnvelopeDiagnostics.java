package gregor.avro;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;
import lombok.NoArgsConstructor;
import lombok.Synchronized;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

@NoArgsConstructor
final class WebhookEnvelopeDiagnostics {

    private static final Set<String> AVRO_SCHEMA_LOGGER_NAMES = Set.of(Schema.class.getName(), LogicalTypes.class.getName(), SchemaCompatibility.class.getName());
    private static final ThreadLocal<Boolean> AVRO_SCHEMA_LOGGING_SUPPRESSED = new ThreadLocal<>();
    @SuppressWarnings("unused")
    private static final Object FILTER_INSTALLATION_LOCK = new Object();

    static <T> T withoutAvroSchemaLogging(Supplier<T> supplier) {
        if (LoggerFactory.getILoggerFactory() instanceof LoggerContext loggerContext) {
            installFilter(loggerContext);
        }
        Boolean previouslySuppressed = AVRO_SCHEMA_LOGGING_SUPPRESSED.get();
        AVRO_SCHEMA_LOGGING_SUPPRESSED.set(true);
        try {
            return supplier.get();
        } finally {
            if (Objects.isNull(previouslySuppressed)) {
                AVRO_SCHEMA_LOGGING_SUPPRESSED.remove();
            } else {
                AVRO_SCHEMA_LOGGING_SUPPRESSED.set(previouslySuppressed);
            }
        }
    }

    @Synchronized("FILTER_INSTALLATION_LOCK")
    private static void installFilter(LoggerContext loggerContext) {
        if (loggerContext.getTurboFilterList().stream().noneMatch(AvroSchemaTurboFilter.class::isInstance)) {
            AvroSchemaTurboFilter avroSchemaTurboFilter = new AvroSchemaTurboFilter();
            avroSchemaTurboFilter.setContext(loggerContext);
            avroSchemaTurboFilter.start();
            loggerContext.getTurboFilterList().addFirst(avroSchemaTurboFilter);
        }
    }

    private static final class AvroSchemaTurboFilter extends TurboFilter {

        @Override
        public FilterReply decide(Marker marker, Logger logger, Level level, String format, Object[] arguments, Throwable throwable) {
            return AVRO_SCHEMA_LOGGER_NAMES.contains(logger.getName()) && Objects.equals(Boolean.TRUE, AVRO_SCHEMA_LOGGING_SUPPRESSED.get()) ? FilterReply.DENY : FilterReply.NEUTRAL;
        }
    }
}
