package gregor.exception;

public class EligibilityWaitInterruptedException extends RuntimeException {

    public EligibilityWaitInterruptedException(Throwable cause) {
        super("Eligibility wait was interrupted", cause);
    }
}
