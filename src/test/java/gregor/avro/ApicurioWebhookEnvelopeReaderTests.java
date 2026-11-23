package gregor.avro;

import gregor.config.KafkaConfiguration;
import gregor.config.properties.KafkaTopicProperties;
import io.apicurio.registry.serde.kafka.headers.KafkaSerdeHeaders;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import org.apache.avro.util.ClassSecurityValidator;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class ApicurioWebhookEnvelopeReaderTests {

    private static final String SCHEMA_PATH = "/apis/registry/v3/ids/globalIds/42";

    @Test
    void closesItsRegistryConnectionWithoutClosingSharedVertx() throws Exception {
        ClassSecurityValidator.ClassSecurityPredicate previousClassSecurityPredicate = ClassSecurityValidator.getGlobal();
        CountDownLatch connectionClosed = new CountDownLatch(1);
        try (AnnotationConfigApplicationContext annotationConfigApplicationContext = new AnnotationConfigApplicationContext()) {
            annotationConfigApplicationContext.register(KafkaConfiguration.class, WebhookEnvelopeCodec.class);
            annotationConfigApplicationContext.registerBean(KafkaTopicProperties.class, () -> new KafkaTopicProperties(1, 1, Duration.ofDays(7), Duration.ofDays(30)));
            new AvroClassSecurityInitializer().initialize(annotationConfigApplicationContext);
            annotationConfigApplicationContext.refresh();

            Vertx registryVertx = annotationConfigApplicationContext.getBean(Vertx.class);
            HttpServer httpServer = registryVertx
                    .createHttpServer()
                    .connectionHandler(httpConnection -> httpConnection.closeHandler(_ -> connectionClosed.countDown()))
                    .requestHandler(ApicurioWebhookEnvelopeReaderTests::servePhaseOneSchema)
                    .listen(0, "127.0.0.1")
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(5, TimeUnit.SECONDS);
            annotationConfigApplicationContext
                    .getEnvironment()
                    .getPropertySources()
                    .addFirst(new MapPropertySource(
                            "fixtureRegistry",
                            Map.of("camel.component.kafka.additional-properties[apicurio.registry.url]", "http://127.0.0.1:" + httpServer.actualPort() + "/apis/registry/v3")
                    ));
            Headers headers = new RecordHeaders().add(KafkaSerdeHeaders.HEADER_VALUE_GLOBAL_ID, ByteBuffer.allocate(Long.BYTES).putLong(42).array());
            byte[] datumBytes = WebhookEnvelopeFixtures.phaseOneDatumWith();
            String readerBeanName = "apicurioWebhookEnvelopeReader";
            ApicurioWebhookEnvelopeReader apicurioWebhookEnvelopeReader = annotationConfigApplicationContext.getBean(readerBeanName, ApicurioWebhookEnvelopeReader.class);
            Assertions.assertThat(apicurioWebhookEnvelopeReader.read("webhook-events-dead-letter", headers, datumBytes).datumBytes()).containsExactly(datumBytes);
            Assertions.assertThat(connectionClosed.getCount()).isEqualTo(1);

            // Destroy only the reader. Keep the context and its Vert.x server running.
            annotationConfigApplicationContext.getDefaultListableBeanFactory().destroySingleton(readerBeanName);
            Assertions.assertThat(connectionClosed.await(5, TimeUnit.SECONDS)).isTrue();
            ApicurioWebhookEnvelopeReader replacementApicurioWebhookEnvelopeReader = annotationConfigApplicationContext.getBean(readerBeanName, ApicurioWebhookEnvelopeReader.class);
            Assertions.assertThat(replacementApicurioWebhookEnvelopeReader.read("webhook-events-dead-letter", headers, datumBytes).datumBytes()).containsExactly(datumBytes);
        } finally {
            ClassSecurityValidator.setGlobal(previousClassSecurityPredicate);
        }
    }

    private static void servePhaseOneSchema(HttpServerRequest httpServerRequest) {
        switch (httpServerRequest.path()) {
            case SCHEMA_PATH -> httpServerRequest.response().putHeader("Content-Type", "application/json").end(WebhookEnvelopeFixtures.PHASE_ONE_WEBHOOK_EVENT_SCHEMA.toString());
            case SCHEMA_PATH + "/references" -> httpServerRequest.response().putHeader("Content-Type", "application/json").end("[]");
            default -> httpServerRequest.response().setStatusCode(404).end();
        }
    }
}
