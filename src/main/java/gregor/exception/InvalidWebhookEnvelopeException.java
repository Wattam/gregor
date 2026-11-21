package gregor.exception;

public class InvalidWebhookEnvelopeException extends IllegalArgumentException {

    public InvalidWebhookEnvelopeException(String message) {
        super(message);
    }
}
