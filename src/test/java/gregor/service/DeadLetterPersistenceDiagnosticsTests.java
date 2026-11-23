package gregor.service;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import gregor.dto.StoredWebhookEnvelope;
import gregor.repository.DeadLetterOccurrenceWriter;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.SQLStateSQLExceptionTranslator;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class DeadLetterPersistenceDiagnosticsTests {

    @Test
    void suppressesOnlyDatabaseStackLogsOnTheScopedThreadAndRestoresAfterFailureAndReset() {
        LoggerContext loggerContext = new LoggerContext();
        loggerContext.start();
        try (
                MockedStatic<LoggerFactory> mockedLoggerFactory = Mockito.mockStatic(LoggerFactory.class, Mockito.CALLS_REAL_METHODS);
                ExecutorService executorService = Executors.newSingleThreadExecutor()
        ) {
            mockedLoggerFactory.when(LoggerFactory::getILoggerFactory).thenReturn(loggerContext);
            List<Logger> databaseLoggers = List.of(
                    loggerContext.getLogger("com.zaxxer.hikari.pool.ProxyConnection"),
                    loggerContext.getLogger("org.hibernate.engine.jdbc.spi.SqlExceptionHelper"),
                    loggerContext.getLogger("org.springframework.jdbc.core.JdbcTemplate"),
                    loggerContext.getLogger("org.springframework.orm.jpa.JpaTransactionManager"),
                    loggerContext.getLogger("org.springframework.transaction.support.AbstractPlatformTransactionManager")
            );
            Logger routeLogger = loggerContext.getLogger("org.apache.camel.component.kafka");
            databaseLoggers.forEach(logger -> logger.setLevel(Level.DEBUG));
            Assertions.assertThat(databaseLoggers).allMatch(Logger::isDebugEnabled);

            Assertions.assertThatIllegalStateException().isThrownBy(() -> DeadLetterPersistenceDiagnostics.withoutRawDatabaseLogging(() -> {
                Assertions.assertThat(databaseLoggers).noneMatch(Logger::isErrorEnabled);
                Assertions.assertThat(routeLogger.isErrorEnabled()).isTrue();
                Assertions.assertThat(executorService.submit(() -> databaseLoggers.stream().allMatch(Logger::isDebugEnabled))).succeedsWithin(5, TimeUnit.SECONDS).isEqualTo(true);
                DeadLetterPersistenceDiagnostics.withoutRawDatabaseLogging(() -> Assertions.assertThat(databaseLoggers).noneMatch(Logger::isErrorEnabled));
                Assertions.assertThat(databaseLoggers).noneMatch(Logger::isErrorEnabled);
                throw new IllegalStateException("scope failure");
            })).withMessage("scope failure");

            Assertions.assertThat(databaseLoggers).allMatch(Logger::isDebugEnabled);
            Assertions.assertThat(loggerContext.getTurboFilterList()).hasSize(1);
            loggerContext.reset();
            Assertions.assertThat(databaseLoggers).allMatch(Logger::isErrorEnabled);
            DeadLetterPersistenceDiagnostics.withoutRawDatabaseLogging(() -> Assertions.assertThat(databaseLoggers).noneMatch(Logger::isErrorEnabled));
            Assertions.assertThat(databaseLoggers).allMatch(Logger::isErrorEnabled);
            Assertions.assertThat(loggerContext.getTurboFilterList()).hasSize(1);
        } finally {
            loggerContext.stop();
        }
    }

    @ParameterizedTest
    @EnumSource
    void removesRawDriverDetailsBeforeJdbcExceptionTranslation(JdbcFailureStage jdbcFailureStage) throws SQLException {
        DataSource dataSource = Mockito.mock(DataSource.class);
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(dataSource.getConnection()).thenReturn(connection);
        Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(preparedStatement);
        SQLException sqlException = new SQLException("private statement parameters", "23514", new IllegalArgumentException("private driver cause"));
        sqlException.addSuppressed(new SQLException("private driver cleanup"));
        switch (jdbcFailureStage) {
            case PREPARE -> Mockito.when(connection.prepareStatement(Mockito.anyString())).thenThrow(sqlException);
            case BIND -> Mockito.doThrow(sqlException).when(preparedStatement).setBytes(Mockito.anyInt(), Mockito.any(byte[].class));
            case EXECUTE -> Mockito.when(preparedStatement.executeUpdate()).thenThrow(sqlException);
            case CLOSE -> Mockito.doThrow(sqlException).when(preparedStatement).close();
        }
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.setExceptionTranslator(new SQLStateSQLExceptionTranslator());
        DeadLetterOccurrenceWriter deadLetterOccurrenceWriter = new DeadLetterOccurrenceWriter(jdbcTemplate);
        UUID occurrenceId = UUID.fromString("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa");
        UUID eventId = UUID.fromString("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb");
        StoredWebhookEnvelope storedWebhookEnvelope = new StoredWebhookEnvelope("{\"private\":\"schema\"}", new byte[]{1, 2, 3});

        Assertions.assertThatThrownBy(() -> deadLetterOccurrenceWriter.insertIfAbsent(occurrenceId, eventId, 0, storedWebhookEnvelope, Instant.EPOCH))
                .satisfies(
                        exception -> Assertions.assertThat(NestedExceptionUtils.getMostSpecificCause(exception))
                                .isInstanceOfSatisfying(SQLException.class, sanitizedSQLException -> {
                                    Assertions.assertThat((Throwable) sanitizedSQLException).hasMessage("Dead-letter occurrence insert failed").hasNoCause();
                                    Assertions.assertThat(sanitizedSQLException.getSQLState()).isEqualTo("23514");
                                    Assertions.assertThat(sanitizedSQLException.getSuppressed()).isEmpty();
                                })
                );
    }

    private enum JdbcFailureStage {
        PREPARE, BIND, EXECUTE, CLOSE
    }
}
