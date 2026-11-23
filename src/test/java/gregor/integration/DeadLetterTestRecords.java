package gregor.integration;

import gregor.avro.WebhookEnvelopeFixtures;
import gregor.avro.WebhookEvent;
import io.apicurio.registry.resolver.client.RegistryArtifactReference;
import io.apicurio.registry.resolver.client.RegistryClientFacade;
import io.apicurio.registry.resolver.client.RegistryClientFacadeFactory;
import io.apicurio.registry.resolver.client.RegistryVersionCoordinates;
import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.vertx.core.Vertx;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.Environment;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class DeadLetterTestRecords {

    static final String GROUP_ID = "webhook-events-dead-letter";
    static final String ROUTE_ID = KafkaTestRecords.consumerRoute(GROUP_ID);
    static final List<String> APPLICATION_TOPICS = List.of("webhook-events", "persist-delivery-attempt", "webhook-events-retry-1m", "webhook-events-retry-10m", "webhook-events-retry-1h", "webhook-events-dead-letter");

    static RegistryClientFacade registry(Environment environment, Vertx registryVertx) {
        Map<String, Object> serdeProperties = new HashMap<>(Binder.get(environment).bind("camel.component.kafka.additional-properties", Bindable.mapOf(String.class, Object.class)).get());
        serdeProperties.put(SchemaResolverConfig.VERTX_INSTANCE, registryVertx);
        return RegistryClientFacadeFactory.create(new SchemaResolverConfig(serdeProperties));
    }

    static RegistryVersionCoordinates registerCurrent(RegistryClientFacade registryClientFacade) {
        return register(registryClientFacade, "current", WebhookEvent.getClassSchema().toString(), Set.of());
    }

    static RegistryVersionCoordinates register(RegistryClientFacade registryClientFacade, String fixtureName, String schemaJson, Set<RegistryArtifactReference> registryArtifactReferences) {
        // Reuse fixture versions across runs. These artifacts are never the application's topic artifacts.
        return registryClientFacade.createSchema("AVRO", "default", "gregor-it-dead-letter-" + fixtureName, null, "FIND_OR_CREATE_VERSION", false, schemaJson, registryArtifactReferences);
    }

    static GenericRecord terminalDatum(String eventId, int replayCount, Instant nextEligibleAt) {
        GenericRecord genericRecord = WebhookEnvelopeFixtures.phaseOneEvent(WebhookEvent.getClassSchema(), eventId);
        genericRecord.put("retryCount", 3);
        genericRecord.put("nextEligibleAt", Objects.isNull(nextEligibleAt) ? null : nextEligibleAt.toEpochMilli());
        genericRecord.put("deliveryAttemptCount", 4);
        genericRecord.put("replayCount", replayCount);
        return genericRecord;
    }

    static byte[] prefix(RegistryVersionCoordinates coordinates, byte[] datumBytes) {
        // Default Apicurio prefix: magic byte, four-byte content ID, then the binary datum.
        return ByteBuffer.allocate(1 + Integer.BYTES + datumBytes.length).put((byte) 0).putInt(Math.toIntExact(coordinates.getContentId())).put(datumBytes).array();
    }

    static Headers idHeader(String headerName, long id) {
        return new RecordHeaders().add(headerName, ByteBuffer.allocate(Long.BYTES).putLong(id).array());
    }

    static RecordMetadata send(KafkaProducer<String, byte[]> kafkaProducer, TopicPartition topicPartition, Instant timestamp, String key, byte[] valueBytes, Headers headers) throws Exception {
        return kafkaProducer.send(new ProducerRecord<>(topicPartition.topic(), topicPartition.partition(), timestamp.toEpochMilli(), key, valueBytes, headers)).get(10, TimeUnit.SECONDS);
    }
}
