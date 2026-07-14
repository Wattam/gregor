package gregor.route;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class BaseRouteTests {

    @Test
    void resolvesProducerAndConsumerKafkaUris() {
        TestRoute testRoute = new TestRoute();

        Assertions.assertThat(testRoute.resolveKafkaProducerUri("webhook-events"))
                .isEqualTo("kafka:webhook-events");
        Assertions.assertThat(testRoute.resolveKafkaConsumerUri("webhook-events"))
                .isEqualTo("kafka:webhook-events?groupId=webhook-events");
        Assertions.assertThat(testRoute.resolveKafkaConsumerUri("persist-delivery-attempt"))
                .isEqualTo("kafka:persist-delivery-attempt?groupId=persist-delivery-attempt");
    }

    private static final class TestRoute extends BaseRoute {

        @Override
        public void configure() {
        }

        private String resolveKafkaProducerUri(String topic) {
            return this.kafkaProducerUri(topic);
        }

        private String resolveKafkaConsumerUri(String topic) {
            return this.kafkaConsumerUri(topic);
        }
    }
}
