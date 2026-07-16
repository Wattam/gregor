package gregor;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
        "gregor.kafka.topics.retry-retention=7d",
        "gregor.kafka.topics.dead-letter-retention=30d"
})
class ApplicationTests {

    @Test
    void contextLoads() {
    }
}
