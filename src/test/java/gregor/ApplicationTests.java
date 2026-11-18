package gregor;

import org.apache.camel.CamelContext;
import org.apache.camel.Route;
import org.apache.camel.ServiceStatus;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
        "camel.component.kafka.max-poll-interval-ms=4500000",
        "gregor.kafka.topics.retry-retention=7d",
        "gregor.kafka.topics.dead-letter-retention=30d"
})
class ApplicationTests {

    @Autowired
    private CamelContext camelContext;

    @Test
    void loadsRoutesWithoutStartingThem() {
        Assertions.assertThat(this.camelContext.getRoutes())
                .isNotEmpty()
                .extracting(Route::getId)
                .allSatisfy(
                        routeId -> Assertions.assertThat(this.camelContext.getRouteController().getRouteStatus(routeId))
                                .isEqualTo(ServiceStatus.Stopped)
                );
    }
}
