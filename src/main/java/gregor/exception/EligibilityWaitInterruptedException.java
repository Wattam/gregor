package gregor.exception;

public class EligibilityWaitInterruptedException extends RuntimeException {

    public EligibilityWaitInterruptedException(Throwable waitFailureThrowable) {
        super("Eligibility wait was interrupted", waitFailureThrowable);
    }
}
