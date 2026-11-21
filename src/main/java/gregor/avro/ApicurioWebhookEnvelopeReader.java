package gregor.avro;

import gregor.dto.StoredWebhookEnvelope;
import gregor.exception.InvalidWebhookEnvelopeException;
import io.apicurio.registry.client.common.DefaultVertxInstance;
import io.apicurio.registry.resolver.DefaultSchemaResolver;
import io.apicurio.registry.resolver.ParsedSchema;
import io.apicurio.registry.resolver.ParsedSchemaImpl;
import io.apicurio.registry.resolver.SchemaLookupResult;
import io.apicurio.registry.resolver.client.RegistryArtifactReference;
import io.apicurio.registry.resolver.client.RegistryClientFacade;
import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.apicurio.registry.resolver.strategy.ArtifactReference;
import io.apicurio.registry.serde.avro.AvroDeserializer;
import io.apicurio.registry.serde.avro.AvroEncoding;
import io.apicurio.registry.serde.avro.AvroSerdeConfig;
import io.apicurio.registry.serde.avro.AvroSerdeHeaders;
import io.apicurio.registry.serde.config.SerdeConfig;
import io.apicurio.registry.serde.kafka.KafkaDeserializer;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.ThreadingModel;
import io.vertx.core.Vertx;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.kafka.common.header.Headers;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class ApicurioWebhookEnvelopeReader implements AutoCloseable {

    private static final String MALFORMED_FRAMING = "Apicurio webhook envelope framing is malformed";
    private static final String MISSING_SCHEMA = "Webhook envelope writer schema is missing";

    private final EnvelopeKafkaDeserializer envelopeKafkaDeserializer;
    private final Vertx registryVertx;
    private final String deploymentId;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AvroEncoding defaultAvroEncoding;
    private final AvroSerdeHeaders avroSerdeHeaders = new AvroSerdeHeaders(false);

    public ApicurioWebhookEnvelopeReader(WebhookEnvelopeCodec webhookEnvelopeCodec, Map<String, ?> serdeProperties) {
        this.defaultAvroEncoding = new AvroSerdeConfig(serdeProperties).getAvroEncoding();
        this.registryVertx = Objects.requireNonNullElseGet(new SchemaResolverConfig(serdeProperties).getVertx(), DefaultVertxInstance::get);
        Map<String, Object> delegateProperties = new HashMap<>(serdeProperties);
        delegateProperties.put(SerdeConfig.CONTRACT_RULES_ENABLED, false);
        delegateProperties.put(SchemaResolverConfig.VERTX_INSTANCE, this.registryVertx);
        EnvelopeAvroDeserializer envelopeAvroDeserializer = new EnvelopeAvroDeserializer(webhookEnvelopeCodec);
        EnvelopeKafkaDeserializer envelopeKafkaDeserializer = new EnvelopeKafkaDeserializer(envelopeAvroDeserializer);
        // Apicurio's resolver does not close its HTTP pools. Create them in a deployment that owns their Vert.x close hooks.
        AbstractVerticle registryVerticle = new AbstractVerticle() {
            @Override
            public void start() {
                envelopeKafkaDeserializer.configure(delegateProperties, false);
                envelopeAvroDeserializer.setFallbackArtifactProvider(null);
            }

            @Override
            public void stop() {
                envelopeKafkaDeserializer.close();
            }
        };
        this.deploymentId = this.registryVertx.deployVerticle(registryVerticle, new DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)).toCompletionStage().toCompletableFuture().join();
        this.envelopeKafkaDeserializer = envelopeKafkaDeserializer;
    }

    public StoredWebhookEnvelope read(String topicName, Headers headers, byte[] valueBytes) {
        if (valueBytes == null || valueBytes.length == 0) {
            throw new InvalidWebhookEnvelopeException(MALFORMED_FRAMING);
        }
        try {
            String encodingHeader = headers == null ? null : this.avroSerdeHeaders.getEncoding(headers);
            AvroEncoding avroEncoding = encodingHeader == null ? this.defaultAvroEncoding : AvroEncoding.valueOf(encodingHeader);
            if (avroEncoding != AvroEncoding.BINARY) {
                throw new InvalidWebhookEnvelopeException("Apicurio webhook envelope must use binary Avro");
            }
            return Objects.requireNonNull(this.envelopeKafkaDeserializer.deserialize(topicName, headers, valueBytes));
        } catch (InvalidWebhookEnvelopeException invalidWebhookEnvelopeException) {
            throw invalidWebhookEnvelopeException;
        } catch (RuntimeException exception) {
            throw new InvalidWebhookEnvelopeException(MALFORMED_FRAMING);
        }
    }

    @Override
    public void close() {
        if (this.closed.compareAndSet(false, true)) {
            this.registryVertx.undeploy(this.deploymentId).toCompletionStage().toCompletableFuture().join();
        }
    }

    private static boolean hasContentHash(ArtifactReference artifactReference) {
        return artifactReference != null && artifactReference.getContentHash() != null && !artifactReference.getContentHash().isBlank();
    }

    private static ArtifactReference selectExactIdentifier(ArtifactReference artifactReference) {
        if (artifactReference != null) {
            if (artifactReference.getContentId() != null) {
                return ArtifactReference.builder().contentId(artifactReference.getContentId()).build();
            }
            if (hasContentHash(artifactReference)) {
                return ArtifactReference.builder().contentHash(artifactReference.getContentHash()).build();
            }
            if (artifactReference.getGlobalId() != null) {
                return ArtifactReference.builder().globalId(artifactReference.getGlobalId()).build();
            }
        }
        throw new InvalidWebhookEnvelopeException(MISSING_SCHEMA);
    }

    private static final class EnvelopeKafkaDeserializer extends KafkaDeserializer<Schema, StoredWebhookEnvelope> {

        private EnvelopeKafkaDeserializer(EnvelopeAvroDeserializer envelopeAvroDeserializer) {
            super(envelopeAvroDeserializer);
        }

        @Override
        public StoredWebhookEnvelope deserialize(String topicName, Headers headers, byte[] valueBytes) {
            if (super.headersHandler == null || headers == null) {
                return super.deserialize(topicName, valueBytes);
            }
            ArtifactReference artifactReference = super.headersHandler.readHeaders(headers);
            if (hasContentHash(artifactReference) && !artifactReference.hasValue()) {
                // Apicurio 3.3.4 omits content hashes from the header-reference presence check.
                return super.delegatedDeserializer.readData(topicName, valueBytes, artifactReference);
            }
            return super.deserialize(topicName, headers, valueBytes);
        }
    }

    private static final class EnvelopeAvroDeserializer extends AvroDeserializer<StoredWebhookEnvelope> {

        private final WebhookEnvelopeCodec webhookEnvelopeCodec;

        private EnvelopeAvroDeserializer(WebhookEnvelopeCodec webhookEnvelopeCodec) {
            super(new EnvelopeSchemaResolver());
            this.webhookEnvelopeCodec = webhookEnvelopeCodec;
        }

        @Override
        protected SchemaLookupResult<Schema> resolve(String topicName, byte[] valueBytes, ArtifactReference artifactReference) {
            ArtifactReference exactArtifactReference = selectExactIdentifier(artifactReference);
            try {
                return Objects.requireNonNull(super.resolve(topicName, valueBytes, exactArtifactReference));
            } catch (RuntimeException | StackOverflowError exception) {
                throw new InvalidWebhookEnvelopeException("Webhook envelope writer schema could not be resolved");
            }
        }

        @Override
        protected StoredWebhookEnvelope readData(ParsedSchema<Schema> parsedSchema, ByteBuffer byteBuffer, int start, int length) {
            Schema writerSchema = parsedSchema == null ? null : parsedSchema.getParsedSchema();
            if (writerSchema == null) {
                throw new InvalidWebhookEnvelopeException(MISSING_SCHEMA);
            }
            Objects.checkFromIndexSize(start - byteBuffer.arrayOffset(), length, byteBuffer.limit());
            return this.webhookEnvelopeCodec.retain(writerSchema, Arrays.copyOfRange(byteBuffer.array(), start, start + length));
        }
    }

    private static final class EnvelopeSchemaResolver extends DefaultSchemaResolver<Schema, StoredWebhookEnvelope> {

        @Override
        public SchemaLookupResult<Schema> resolveSchemaByArtifactReference(ArtifactReference artifactReference) {
            // Bypass root cache loaders that log the original lookup exception. The deserializer still caches exact numeric IDs.
            RegistryClientFacade registryClientFacade = super.getClientFacade();
            SchemaLookupResult.SchemaLookupResultBuilder<Schema> schemaLookupResultBuilder = SchemaLookupResult.builder();
            String writerSchemaJson;
            List<RegistryArtifactReference> registryArtifactReferences;
            if (artifactReference.getContentId() != null) {
                long contentId = artifactReference.getContentId();
                writerSchemaJson = registryClientFacade.getSchemaByContentId(contentId);
                registryArtifactReferences = registryClientFacade.getReferencesByContentId(contentId);
                schemaLookupResultBuilder.contentId(contentId);
            } else if (artifactReference.getContentHash() != null) {
                String contentHash = artifactReference.getContentHash();
                writerSchemaJson = registryClientFacade.getSchemaByContentHash(contentHash);
                registryArtifactReferences = registryClientFacade.getReferencesByContentHash(contentHash);
                schemaLookupResultBuilder.contentHash(contentHash);
            } else {
                long globalId = artifactReference.getGlobalId();
                writerSchemaJson = registryClientFacade.getSchemaByGlobalId(globalId, false);
                registryArtifactReferences = registryClientFacade.getReferencesByGlobalId(globalId);
                schemaLookupResultBuilder.globalId(globalId);
            }
            Schema writerSchema = WebhookEnvelopeDiagnostics.withoutAvroSchemaLogging(() -> new EnvelopeReferenceResolver(registryClientFacade).parse(writerSchemaJson, registryArtifactReferences).schema());
            return schemaLookupResultBuilder.parsedSchema(new ParsedSchemaImpl<Schema>().setParsedSchema(writerSchema)).build();
        }
    }

    private static final class EnvelopeReferenceResolver {

        private final RegistryClientFacade registryClientFacade;
        private final Map<ReferenceVersion, Map<String, Schema>> resolvedReferences = new HashMap<>();
        private final Set<ReferenceVersion> activeReferences = new HashSet<>();

        private EnvelopeReferenceResolver(RegistryClientFacade registryClientFacade) {
            this.registryClientFacade = registryClientFacade;
        }

        private ResolvedSchema parse(String schemaJson, List<RegistryArtifactReference> registryArtifactReferences) {
            WebhookEnvelopeSchemas.requireSchemaJson(schemaJson);
            Map<String, Schema> namedSchemas = new HashMap<>();
            for (RegistryArtifactReference registryArtifactReference : registryArtifactReferences) {
                this.resolve(registryArtifactReference).values().forEach(schema -> WebhookEnvelopeSchemas.mergeDefinition(namedSchemas, schema));
            }

            Schema.Parser parser = WebhookEnvelopeSchemas.parser(namedSchemas);
            Schema schema = parser.parse(schemaJson);
            WebhookEnvelopeSchemas.mergeNamedDefinitions(namedSchemas, parser.getTypes().values());
            return new ResolvedSchema(schema, namedSchemas);
        }

        private Map<String, Schema> resolve(RegistryArtifactReference registryArtifactReference) {
            String groupId = Objects.requireNonNullElse(registryArtifactReference.getGroupId(), "default");
            String artifactId = registryArtifactReference.getArtifactId();
            String version = registryArtifactReference.getVersion();
            if (artifactId == null || artifactId.isBlank() || version == null || version.isBlank()) {
                throw new SchemaParseException("Avro named reference has no artifact version");
            }
            ReferenceVersion referenceVersion = new ReferenceVersion(groupId, artifactId, version);
            Map<String, Schema> namedSchemas = this.resolvedReferences.get(referenceVersion);
            if (namedSchemas != null) {
                return namedSchemas;
            }
            if (!this.activeReferences.add(referenceVersion)) {
                throw new SchemaParseException("Avro named reference is cyclic");
            }
            try {
                String schemaJson = this.registryClientFacade.getSchemaByGAV(groupId, artifactId, version);
                List<RegistryArtifactReference> registryArtifactReferences = this.registryClientFacade.getReferencesByGAV(groupId, artifactId, version);
                namedSchemas = this.parse(schemaJson, registryArtifactReferences).namedSchemas();
                this.resolvedReferences.put(referenceVersion, namedSchemas);
                return namedSchemas;
            } finally {
                this.activeReferences.remove(referenceVersion);
            }
        }

        private record ReferenceVersion(String groupId, String artifactId, String version) {
        }

        private record ResolvedSchema(Schema schema, Map<String, Schema> namedSchemas) {
        }
    }
}
