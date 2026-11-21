package gregor.avro;

import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.apicurio.registry.serde.kafka.config.KafkaSerdeConfig;
import io.apicurio.registry.serde.kafka.headers.KafkaSerdeHeaders;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import org.apache.avro.util.ClassSecurityValidator;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.GenericApplicationContext;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class ApicurioWebhookEnvelopeReaderTests {

    private static final String SCHEMA_PATH = "/apis/registry/v3/ids/globalIds/42";

    @Test
    void closesItsRegistryConnectionWithoutClosingSharedVertx() throws Exception {
        ClassSecurityValidator.ClassSecurityPredicate previousClassSecurityPredicate = ClassSecurityValidator.getGlobal();
        Vertx registryVertx = Vertx.vertx();
        CountDownLatch connectionClosed = new CountDownLatch(1);
        try (GenericApplicationContext genericApplicationContext = new GenericApplicationContext()) {
            new AvroClassSecurityInitializer().initialize(genericApplicationContext);
            HttpServer httpServer = registryVertx
                    .createHttpServer()
                    .connectionHandler(httpConnection -> httpConnection.closeHandler(_ -> connectionClosed.countDown()))
                    .requestHandler(ApicurioWebhookEnvelopeReaderTests::servePhaseOneSchema)
                    .listen(0, "127.0.0.1")
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(5, TimeUnit.SECONDS);
            Map<String, Object> serdeProperties = Map.of(
                    SchemaResolverConfig.REGISTRY_URL, "http://127.0.0.1:" + httpServer.actualPort() + "/apis/registry/v3",
                    SchemaResolverConfig.VERTX_INSTANCE, registryVertx,
                    KafkaSerdeConfig.ENABLE_HEADERS, true
            );
            Headers headers = new RecordHeaders().add(KafkaSerdeHeaders.HEADER_VALUE_GLOBAL_ID, ByteBuffer.allocate(Long.BYTES).putLong(42).array());
            byte[] datumBytes = WebhookEnvelopeFixtures.phaseOneDatumWith();
            WebhookEnvelopeCodec webhookEnvelopeCodec = new WebhookEnvelopeCodec();

            try (ApicurioWebhookEnvelopeReader apicurioWebhookEnvelopeReader = new ApicurioWebhookEnvelopeReader(webhookEnvelopeCodec, serdeProperties)) {
                Assertions.assertThat(apicurioWebhookEnvelopeReader.read("webhook-events-dead-letter", headers, datumBytes).datumBytes()).containsExactly(datumBytes);
                Assertions.assertThat(connectionClosed.getCount()).isEqualTo(1);
            }
            Assertions.assertThat(connectionClosed.await(5, TimeUnit.SECONDS)).isTrue();
            try (ApicurioWebhookEnvelopeReader apicurioWebhookEnvelopeReader = new ApicurioWebhookEnvelopeReader(webhookEnvelopeCodec, serdeProperties)) {
                Assertions.assertThat(apicurioWebhookEnvelopeReader.read("webhook-events-dead-letter", headers, datumBytes).datumBytes()).containsExactly(datumBytes);
            }
        } finally {
            ClassSecurityValidator.setGlobal(previousClassSecurityPredicate);
            registryVertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
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
