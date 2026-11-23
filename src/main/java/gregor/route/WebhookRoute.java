package gregor.route;

import gregor.avro.DeliveryAttempt;
import gregor.avro.WebhookEvent;
import gregor.dto.RetryPublication;
import gregor.dto.WebhookRouteMessage;
import gregor.service.WebhookService;
import lombok.AllArgsConstructor;
import org.apache.camel.Exchange;
import org.apache.camel.InvalidPayloadException;
import org.apache.camel.Message;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.processor.aggregate.UseOriginalAggregationStrategy;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@AllArgsConstructor
public class WebhookRoute extends BaseRoute {

    public static final String WEBHOOK_EVENT_TOPIC = "webhook-events";
    public static final String WEBHOOK_DELIVERY_ATTEMPT_DIRECT = "direct:webhook-delivery-attempt";
    public static final String WEBHOOK_DELIVERY_RESULT_DIRECT = "direct:webhook-delivery-result";
    public static final String WEBHOOK_RETRY_ELIGIBILITY_DIRECT = "direct:webhook-retry-eligibility";
    public static final String PERSIST_DELIVERY_ATTEMPT_TOPIC = "persist-delivery-attempt";
    public static final String WEBHOOK_EVENT_RETRY_1M_TOPIC = "webhook-events-retry-1m";
    public static final String WEBHOOK_EVENT_RETRY_10M_TOPIC = "webhook-events-retry-10m";
    public static final String WEBHOOK_EVENT_RETRY_1H_TOPIC = "webhook-events-retry-1h";
    public static final String WEBHOOK_EVENT_DEAD_LETTER_TOPIC = "webhook-events-dead-letter";

    private final WebhookService webhookService;

    @Override
    public void configure() {
        super.configureErrorHandler();

        // @formatter:off
        super.routeDefinition(super.kafkaConsumerUri(WEBHOOK_EVENT_TOPIC), WebhookEvent.class)
            .bean(this.webhookService, "createWebhookRouteMessage")
            .to(WEBHOOK_DELIVERY_ATTEMPT_DIRECT)
            .process(super::commitOffset);

        super.routeDefinition(super.kafkaConsumerUri(WEBHOOK_EVENT_RETRY_1M_TOPIC), WebhookEvent.class)
            .bean(this.webhookService, "createWebhookRouteMessage")
            .to(WEBHOOK_RETRY_ELIGIBILITY_DIRECT)
            .process(super::commitOffset);

        super.routeDefinition(super.kafkaConsumerUri(WEBHOOK_EVENT_RETRY_10M_TOPIC), WebhookEvent.class)
            .bean(this.webhookService, "createWebhookRouteMessage")
            .to(WEBHOOK_RETRY_ELIGIBILITY_DIRECT)
            .process(super::commitOffset);

        super.routeDefinition(super.kafkaConsumerUri(WEBHOOK_EVENT_RETRY_1H_TOPIC), WebhookEvent.class)
            .bean(this.webhookService, "createWebhookRouteMessage")
            .to(WEBHOOK_RETRY_ELIGIBILITY_DIRECT)
            .process(super::commitOffset);

        super.routeDefinition(WEBHOOK_RETRY_ELIGIBILITY_DIRECT, WebhookRouteMessage.class)
            .choice().when().method(this.webhookService, "hasMalformedRetryEnvelope")
                .bean(this.webhookService, "logDiscardedRetryEnvelope")
            .otherwise()
                .bean(this.webhookService, "awaitEligibility")
                .to(WEBHOOK_DELIVERY_ATTEMPT_DIRECT)
            .end();

        super.routeDefinition(WEBHOOK_DELIVERY_ATTEMPT_DIRECT, WebhookRouteMessage.class)
            .choice().when().method(this.webhookService, "hasImpossibleDeliveryState")
                .bean(this.webhookService, "logDiscardedEnvelope")
            .otherwise()
                .bean(this.webhookService, "sendWebhookEvent")
                .to(WEBHOOK_DELIVERY_RESULT_DIRECT)
            .end();

        super.routeDefinition(WEBHOOK_DELIVERY_RESULT_DIRECT, WebhookRouteMessage.class)
            .multicast(new UseOriginalAggregationStrategy()).stopOnException().synchronous()
                .pipeline()
                    .filter().method(this.webhookService, "requiresDeadLetter")
                        .bean(this.webhookService, "publishDeadLetter")
                    .end()
                .end()
                .pipeline()
                    .bean(this.webhookService, "createAttempt")
                    .process(this::prepareAttemptPublication)
                    .to(super.kafkaProducerUri(PERSIST_DELIVERY_ATTEMPT_TOPIC))
                .end()
                .pipeline()
                    .bean(this.webhookService, "planRetry")
                    .filter().body(Optional.class, Optional::isPresent)
                        .process(this::prepareRetryPublication)
                        .to(super.kafkaProducerUri(WEBHOOK_EVENT_RETRY_1M_TOPIC))
                    .end()
                .end()
            .end();

        super.routeDefinition(super.kafkaConsumerUri(PERSIST_DELIVERY_ATTEMPT_TOPIC), DeliveryAttempt.class)
            .bean(this.webhookService, "persistDeliveryAttempt")
            .process(super::commitOffset);
        // @formatter:on
    }

    private void prepareAttemptPublication(Exchange exchange) throws InvalidPayloadException {
        Message message = exchange.getMessage();
        DeliveryAttempt deliveryAttempt = message.getMandatoryBody(DeliveryAttempt.class);
        message.removeHeaders("*");
        message.setHeader(KafkaConstants.KEY, deliveryAttempt.getEventId());
    }

    private void prepareRetryPublication(Exchange exchange) throws InvalidPayloadException {
        Message message = exchange.getMessage();
        RetryPublication retryPublication = (RetryPublication) message.getMandatoryBody(Optional.class).orElseThrow();
        WebhookEvent webhookEvent = retryPublication.webhookEvent();
        message.removeHeaders("*");
        message.setHeader(KafkaConstants.OVERRIDE_TOPIC, retryPublication.topic());
        message.setBody(webhookEvent);
        message.setHeader(KafkaConstants.KEY, webhookEvent.getEndpointId());
    }
}
