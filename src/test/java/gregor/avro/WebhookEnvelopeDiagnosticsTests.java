package gregor.avro;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class WebhookEnvelopeDiagnosticsTests {

    @Test
    void suppressesAvroDiagnosticsOnlyOnTheScopedThreadAndRestoresScopeAfterFailure() {
        Logger schemaLogger = (Logger) LoggerFactory.getLogger(Schema.class);
        Logger logicalTypesLogger = (Logger) LoggerFactory.getLogger(LogicalTypes.class);
        Logger compatibilityLogger = (Logger) LoggerFactory.getLogger(SchemaCompatibility.class);
        Logger unrelatedLogger = (Logger) LoggerFactory.getLogger(WebhookEnvelopeDiagnosticsTests.class);
        Level previousLevel = compatibilityLogger.getLevel();
        compatibilityLogger.setLevel(Level.DEBUG);
        try (ExecutorService executorService = Executors.newSingleThreadExecutor()) {
            Assertions.assertThat(schemaLogger.isWarnEnabled()).isTrue();
            Assertions.assertThat(logicalTypesLogger.isWarnEnabled()).isTrue();
            Assertions.assertThat(compatibilityLogger.isDebugEnabled()).isTrue();
            Assertions.assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> WebhookEnvelopeDiagnostics.withoutAvroSchemaLogging(() -> {
                Assertions.assertThat(schemaLogger.isWarnEnabled()).isFalse();
                Assertions.assertThat(logicalTypesLogger.isWarnEnabled()).isFalse();
                Assertions.assertThat(compatibilityLogger.isDebugEnabled()).isFalse();
                Assertions.assertThat(unrelatedLogger.isWarnEnabled()).isTrue();
                Future<Boolean> unscopedDebugEnabled = executorService.submit(() -> compatibilityLogger.isDebugEnabled());
                Assertions.assertThat(unscopedDebugEnabled).succeedsWithin(5, TimeUnit.SECONDS).isEqualTo(true);
                boolean nestedDebugEnabled = WebhookEnvelopeDiagnostics.withoutAvroSchemaLogging(compatibilityLogger::isDebugEnabled);
                Assertions.assertThat(nestedDebugEnabled).isFalse();
                Assertions.assertThat(compatibilityLogger.isDebugEnabled()).isFalse();
                throw new IllegalStateException("scope failure");
            })).withMessage("scope failure");
            Assertions.assertThat(schemaLogger.isWarnEnabled()).isTrue();
            Assertions.assertThat(logicalTypesLogger.isWarnEnabled()).isTrue();
            Assertions.assertThat(compatibilityLogger.isDebugEnabled()).isTrue();
        } finally {
            compatibilityLogger.setLevel(previousLevel);
        }
    }

    @Test
    void suppressesAvroDiagnosticsAfterTheLoggerContextResets() {
        LoggerContext loggerContext = new LoggerContext();
        loggerContext.start();
        try (MockedStatic<LoggerFactory> mockedLoggerFactory = Mockito.mockStatic(LoggerFactory.class, Mockito.CALLS_REAL_METHODS)) {
            mockedLoggerFactory.when(LoggerFactory::getILoggerFactory).thenReturn(loggerContext);
            Logger schemaLogger = loggerContext.getLogger(Schema.class.getName());
            boolean scopedWarnEnabledBeforeReset = WebhookEnvelopeDiagnostics.withoutAvroSchemaLogging(schemaLogger::isWarnEnabled);
            Assertions.assertThat(scopedWarnEnabledBeforeReset).isFalse();

            loggerContext.reset();

            Assertions.assertThat(schemaLogger.isWarnEnabled()).isTrue();
            boolean scopedWarnEnabledAfterReset = WebhookEnvelopeDiagnostics.withoutAvroSchemaLogging(schemaLogger::isWarnEnabled);
            Assertions.assertThat(scopedWarnEnabledAfterReset).isFalse();
            Assertions.assertThat(schemaLogger.isWarnEnabled()).isTrue();
        } finally {
            loggerContext.stop();
        }
    }
}
