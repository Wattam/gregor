package gregor.exception;

public class EventPublicationException extends RuntimeException {

    public EventPublicationException(Throwable publicationFailureThrowable) {
        super("Event publication failed", publicationFailureThrowable);
    }
}
