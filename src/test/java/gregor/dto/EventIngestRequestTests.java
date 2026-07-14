package gregor.dto;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.NullNode;

import java.util.List;

class EventIngestRequestTests {

    @Test
    void acceptsEveryNonNullJsonPayloadTypeWithANonblankEventType() {
        List<JsonNode> payloads = List.of(
                JsonNodeFactory.instance.objectNode().put("orderId", "123"),
                JsonNodeFactory.instance.arrayNode().add(1),
                JsonNodeFactory.instance.stringNode("value"),
                JsonNodeFactory.instance.numberNode(42),
                JsonNodeFactory.instance.booleanNode(true)
        );

        try (ValidatorFactory factory = Validation.buildDefaultValidatorFactory()) {
            Validator validator = factory.getValidator();
            for (JsonNode payload : payloads) {
                EventIngestRequest request = new EventIngestRequest("order.created", payload);
                Assertions.assertThat(validator.validate(request)).isEmpty();
            }
        }
    }

    @Test
    void rejectsMissingJsonNullAndBlankRequiredValues() {
        try (ValidatorFactory validatorFactory = Validation.buildDefaultValidatorFactory()) {
            Validator validator = validatorFactory.getValidator();

            Assertions.assertThat(validator.validate(new EventIngestRequest("order.created", null))).isNotEmpty();
            Assertions.assertThat(validator.validate(new EventIngestRequest("order.created", NullNode.getInstance())))
                    .isNotEmpty();
            Assertions.assertThat(validator.validate(
                    new EventIngestRequest(" ", JsonNodeFactory.instance.objectNode())
            )).isNotEmpty();
            Assertions.assertThat(validator.validate(
                    new EventIngestRequest(null, JsonNodeFactory.instance.objectNode())
            )).isNotEmpty();
            Assertions.assertThat(validator.validate(
                    new EventIngestRequest("order.\r\ncreated", JsonNodeFactory.instance.objectNode())
            )).isNotEmpty();
            Assertions.assertThat(validator.validate(
                    new EventIngestRequest("order.\u0000created", JsonNodeFactory.instance.objectNode())
            )).isNotEmpty();
        }
    }
}
