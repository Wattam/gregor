package gregor.route;

import gregor.service.DeadLetterOccurrenceService;
import lombok.RequiredArgsConstructor;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@RequiredArgsConstructor
public class DeadLetterOccurrenceRoute extends BaseRoute {

    private final DeadLetterOccurrenceService deadLetterOccurrenceService;

    @Override
    public void configure() {
        super.configureErrorHandler();
        String routeId = super.kafkaConsumerUri(WebhookRoute.WEBHOOK_EVENT_DEAD_LETTER_TOPIC);
        String consumerUri = routeId + "&valueDeserializer=" + ByteArrayDeserializer.class.getName() + "&kafkaClientFactory=#bean:deadLetterKafkaClientFactory";

        // @formatter:off
        super.routeDefinition(consumerUri, routeId, byte[].class)
            .process(this::persistOccurrence)
            .process(super::commitOffset);
        // @formatter:on
    }

    private void persistOccurrence(Exchange exchange) {
        Message message = exchange.getMessage();
        Instant deadLetteredAt = this.deadLetterOccurrenceService.requireKafkaTimestamp(message.getHeader(KafkaConstants.TIMESTAMP));
        if (!(message.getHeader(KafkaConstants.MANUAL_COMMIT) instanceof KafkaManualCommit)) {
            throw new IllegalStateException("Dead-letter Kafka manual commit is missing");
        }

        this.deadLetterOccurrenceService.persistKafkaRecord((String) message.getHeader(KafkaConstants.TOPIC), (Headers) message.getHeader(KafkaConstants.HEADERS), (byte[]) message.getBody(), deadLetteredAt);
    }
}
