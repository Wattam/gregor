package gregor.avro;

import org.apache.avro.util.ClassSecurityValidator;
import org.jspecify.annotations.NonNull;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

public class AvroClassSecurityInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

    @Override
    public void initialize(@NonNull ConfigurableApplicationContext configurableApplicationContext) {
        ClassSecurityValidator.ClassSecurityPredicate avroClassSecurityPredicate = ClassSecurityValidator.builder().add(WebhookEvent.class).add(DeliveryAttempt.class).add(DeliveryOutcome.class).build();
        ClassSecurityValidator.setGlobal(ClassSecurityValidator.composite(ClassSecurityValidator.DEFAULT, avroClassSecurityPredicate));
    }
}
