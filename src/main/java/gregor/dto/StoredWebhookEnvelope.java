package gregor.dto;

import gregor.exception.InvalidWebhookEnvelopeException;
import org.apache.commons.lang3.StringUtils;
import org.jspecify.annotations.NonNull;

import java.util.Arrays;
import java.util.Objects;

public record StoredWebhookEnvelope(String writerSchemaJson, byte[] datumBytes) {

    public StoredWebhookEnvelope {
        if (StringUtils.isBlank(writerSchemaJson)) {
            throw new InvalidWebhookEnvelopeException("Webhook envelope writer schema is missing");
        }
        if (Objects.isNull(datumBytes) || datumBytes.length == 0) {
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
        return other instanceof StoredWebhookEnvelope storedWebhookEnvelope && Objects.equals(this.writerSchemaJson, storedWebhookEnvelope.writerSchemaJson) && Arrays.equals(this.datumBytes, storedWebhookEnvelope.datumBytes);
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
