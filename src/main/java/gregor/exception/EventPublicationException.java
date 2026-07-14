package gregor.exception;

public class EventPublicationException extends RuntimeException {

    public EventPublicationException(Throwable cause) {
        super("Event publication failed", cause);
    }
}
