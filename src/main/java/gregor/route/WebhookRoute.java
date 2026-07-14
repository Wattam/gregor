package gregor.route;

import gregor.avro.DeliveryAttempt;
import gregor.avro.WebhookEvent;
import gregor.dto.WebhookRouteMessage;
import gregor.service.WebhookService;
import lombok.AllArgsConstructor;
import org.apache.camel.component.kafka.KafkaConstants;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class WebhookRoute extends BaseRoute {

    public static final String WEBHOOK_EVENT_TOPIC = "webhook-events";
    public static final String WEBHOOK_DELIVERY_ATTEMPT_DIRECT = "direct:webhook-delivery-attempt";
    public static final String PERSIST_DELIVERY_ATTEMPT_TOPIC = "persist-delivery-attempt";

    private final WebhookService webhookService;

    @Override
    public void configure() {
        super.configureErrorHandler();

        // @formatter:off
        super.routeDefinition(super.kafkaConsumerUri(WEBHOOK_EVENT_TOPIC), WebhookEvent.class)
            .bean(this.webhookService, "createWebhookRouteMessage")
            .to(WEBHOOK_DELIVERY_ATTEMPT_DIRECT)
            .process(super::commitOffset);

        super.routeDefinition(WEBHOOK_DELIVERY_ATTEMPT_DIRECT, WebhookRouteMessage.class)
            .bean(this.webhookService, "sendWebhookEvent")
            .multicast().stopOnException()
                .pipeline()
                    .bean(this.webhookService, "createAttempt")
                    .removeHeaders("*")
                    .setHeader(KafkaConstants.KEY).simple("${body.eventId}")
                    .to(super.kafkaProducerUri(PERSIST_DELIVERY_ATTEMPT_TOPIC))
                .end()
                .pipeline()
                    .choice().when().method(this.webhookService, "shouldRetry")
                        .to(WEBHOOK_DELIVERY_ATTEMPT_DIRECT)
                    .end()
                .end()
            .end();

        super.routeDefinition(super.kafkaConsumerUri(PERSIST_DELIVERY_ATTEMPT_TOPIC), DeliveryAttempt.class)
            .bean(this.webhookService, "persistDeliveryAttempt")
            .process(super::commitOffset);
        // @formatter:on
    }
}
