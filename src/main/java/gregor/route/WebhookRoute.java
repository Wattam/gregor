package gregor.route;

import gregor.avro.DeliveryAttempt;
import gregor.avro.WebhookEvent;
import gregor.dto.RetryPublication;
import gregor.dto.WebhookRouteMessage;
import gregor.service.WebhookService;
import lombok.AllArgsConstructor;
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
                    .bean(this.webhookService, "createAttempt")
                    .removeHeaders("*")
                    .setHeader(KafkaConstants.KEY).body(DeliveryAttempt.class, DeliveryAttempt::getEventId)
                    .to(super.kafkaProducerUri(PERSIST_DELIVERY_ATTEMPT_TOPIC))
                .end()
                .pipeline()
                    .bean(this.webhookService, "planRetry")
                    .filter().body(Optional.class, Optional::isPresent)
                        .setBody().body(Optional.class, Optional::get)
                        .removeHeaders("*")
                        .setHeader(KafkaConstants.OVERRIDE_TOPIC).body(RetryPublication.class, RetryPublication::topic)
                        .setBody().body(RetryPublication.class, RetryPublication::webhookEvent)
                        .setHeader(KafkaConstants.KEY).body(WebhookEvent.class, WebhookEvent::getEndpointId)
                        .to(super.kafkaProducerUri(WEBHOOK_EVENT_RETRY_1M_TOPIC))
                    .end()
                .end()
            .end();

        super.routeDefinition(super.kafkaConsumerUri(PERSIST_DELIVERY_ATTEMPT_TOPIC), DeliveryAttempt.class)
            .bean(this.webhookService, "persistDeliveryAttempt")
            .process(super::commitOffset);
        // @formatter:on
    }
}
