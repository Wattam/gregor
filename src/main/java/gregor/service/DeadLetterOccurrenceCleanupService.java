package gregor.service;

import gregor.config.properties.DeadLetterCleanupProperties;
import gregor.repository.DeadLetterOccurrenceCleanupRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

@Service
@RequiredArgsConstructor
@Slf4j
public class DeadLetterOccurrenceCleanupService {

    private final DeadLetterOccurrenceCleanupRepository deadLetterOccurrenceCleanupRepository;
    private final DeadLetterCleanupProperties deadLetterCleanupProperties;
    private final DeadLetterRetention deadLetterRetention;
    private final DeadLetterCleanupMetrics deadLetterCleanupMetrics;
    private final PlatformTransactionManager platformTransactionManager;

    public void runCleanup() {
        this.deleteBatch();
        this.sampleRetained();
    }

    private void deleteBatch() {
        int deletedCount;
        try {
            deletedCount = this.inNewTransaction(false, () -> this.deadLetterOccurrenceCleanupRepository.deleteExpired(this.deadLetterRetention.cutoff(), this.deadLetterCleanupProperties.batchSize()));
        } catch (RuntimeException exception) {
            this.deadLetterCleanupMetrics.recordFailedBatch();
            log.warn("Dead-letter cleanup failed [outcome=failure]");
            return;
        }
        this.deadLetterCleanupMetrics.recordSuccessfulBatch(deletedCount);
        log.info("Dead-letter cleanup completed [outcome=success, deleted={}]", deletedCount);
    }

    private void sampleRetained() {
        long retainedCount;
        try {
            retainedCount = this.inNewTransaction(true, () -> this.deadLetterOccurrenceCleanupRepository.countRetained(this.deadLetterRetention.cutoff()));
        } catch (RuntimeException exception) {
            log.warn("Dead-letter retained count failed [sample=unchanged]");
            return;
        }
        this.deadLetterCleanupMetrics.recordRetainedCount(retainedCount);
        log.info("Sampled retained dead-letter occurrences [retained={}]", retainedCount);
    }

    private <T> T inNewTransaction(boolean readOnly, Supplier<T> supplier) {
        TransactionTemplate transactionTemplate = new TransactionTemplate(this.platformTransactionManager);
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        transactionTemplate.setReadOnly(readOnly);
        AtomicBoolean committed = new AtomicBoolean();
        T result = DeadLetterPersistenceDiagnostics.withoutRawDatabaseLogging(() -> transactionTemplate.execute(_ -> {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCompletion(int status) {
                    if (status == TransactionSynchronization.STATUS_COMMITTED) {
                        committed.set(true);
                    }
                }
            });
            return supplier.get();
        }));
        // TransactionTemplate also returns normally after a rollback-only transaction.
        if (!committed.get()) {
            throw new IllegalStateException("Dead-letter cleanup transaction did not confirm commit");
        }
        return result;
    }
}
