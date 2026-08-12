package gregor;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
        "camel.component.kafka.max-poll-interval-ms=4500000",
        "gregor.kafka.topics.retry-retention=7d",
        "gregor.kafka.topics.dead-letter-retention=30d"
})
class ApplicationTests {

    @Test
    void contextLoads() {
    }
}
