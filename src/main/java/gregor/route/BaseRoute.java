package gregor.route;

import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.model.RouteDefinition;

import java.util.Objects;

public abstract class BaseRoute extends RouteBuilder {

    protected void configureErrorHandler() {
        super.errorHandler(super.defaultErrorHandler().disableRedelivery());
        super.onException(Exception.class).maximumRedeliveries(0).logStackTrace(true).log(LoggingLevel.ERROR, "Route processing failed [routeId=${routeId}]");
    }

    protected RouteDefinition routeDefinition(String uri, Class<?> inputType) {
        return this.routeDefinition(uri, uri, inputType);
    }

    protected RouteDefinition routeDefinition(String uri, String routeId, Class<?> inputType) {
        return super.from(uri).routeId(routeId).inputType(inputType);
    }

    protected String kafkaProducerUri(String topicName) {
        return "kafka:" + topicName;
    }

    protected String kafkaConsumerUri(String topicName) {
        return this.kafkaProducerUri(topicName) + "?groupId=" + topicName;
    }

    protected void commitOffset(Exchange exchange) {
        KafkaManualCommit kafkaManualCommit = exchange.getMessage().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
        if (Objects.nonNull(kafkaManualCommit)) {
            kafkaManualCommit.commit();
        }
    }
}
