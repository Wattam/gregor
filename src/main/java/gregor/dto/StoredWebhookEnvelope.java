package gregor.dto;

import gregor.exception.InvalidWebhookEnvelopeException;
import org.jspecify.annotations.NonNull;

import java.util.Arrays;

public record StoredWebhookEnvelope(String writerSchemaJson, byte[] datumBytes) {

    public StoredWebhookEnvelope {
        if (writerSchemaJson == null || writerSchemaJson.isBlank()) {
            throw new InvalidWebhookEnvelopeException("Webhook envelope writer schema is missing");
        }
        if (datumBytes == null || datumBytes.length == 0) {
            throw new InvalidWebhookEnvelopeException("Webhook envelope datum is malformed");
        }
        datumBytes = datumBytes.clone();
    }

    @Override
    public byte[] datumBytes() {
        return this.datumBytes.clone();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof StoredWebhookEnvelope storedWebhookEnvelope && this.writerSchemaJson.equals(storedWebhookEnvelope.writerSchemaJson) && Arrays.equals(this.datumBytes, storedWebhookEnvelope.datumBytes);
    }

    @Override
    public int hashCode() {
        return 31 * this.writerSchemaJson.hashCode() + Arrays.hashCode(this.datumBytes);
    }

    @Override
    public @NonNull String toString() {
        return "StoredWebhookEnvelope[redacted]";
    }
}
