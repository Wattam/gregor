package gregor.dto;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import tools.jackson.databind.JsonNode;

public record EventIngestRequest(@NotBlank String eventType, @NotNull JsonNode payload) {

    @AssertTrue(message = "eventType must be safe for an HTTP header")
    public boolean isEventTypeHttpHeaderSafe() {
        return this.eventType == null || this.eventType.codePoints().noneMatch(Character::isISOControl);
    }

    @AssertTrue(message = "payload must not be JSON null")
    public boolean isPayloadNotJsonNull() {
        return this.payload == null || !this.payload.isNull();
    }
}
