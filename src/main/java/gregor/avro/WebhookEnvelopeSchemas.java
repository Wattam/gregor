package gregor.avro;

import lombok.NoArgsConstructor;
import org.apache.avro.ParseContext;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.core.ObjectWriteContext;
import tools.jackson.core.json.JsonFactory;

import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@NoArgsConstructor
final class WebhookEnvelopeSchemas {

    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    static Schema.Parser parser(Map<String, Schema> referencedSchemas) {
        return new Schema.Parser(new EnvelopeParseContext(referencedSchemas));
    }

    static Schema parseSnapshot(String schemaJson) {
        requireSchemaJson(schemaJson);
        return WebhookEnvelopeDiagnostics.withoutAvroSchemaLogging(() -> parser(Map.of()).parse(schemaJson));
    }

    static void requireSchemaJson(String schemaJson) {
        // Avro allows comments and trims non-JSON whitespace after the root value.
        try (JsonParser jsonParser = JSON_FACTORY.createParser(ObjectReadContext.empty(), schemaJson)) {
            if (jsonParser.nextToken() == null || jsonParser.skipChildren().nextToken() != null) {
                throw new SchemaParseException("Expected one Avro JSON schema");
            }
        }
    }

    static Snapshot snapshot(Schema writerSchema) {
        // Avro emits only one JSON definition per full name.
        Map<String, Schema> namedSchemas = new HashMap<>();
        mergeNamedDefinitions(namedSchemas, List.of(writerSchema));
        String schemaJson = writerSchema.toString();
        if (namedSchemas.values().stream().anyMatch(schema -> schema.getNamespace() == null || schema.getAliases().stream().anyMatch(alias -> alias.startsWith(".")))) {
            schemaJson = writeExplicitEmptyNamespaceNames(writerSchema, schemaJson);
        }

        Schema snapshotSchema = parseSnapshot(schemaJson);
        Map<String, Schema> snapshotNamedSchemas = new HashMap<>();
        mergeNamedDefinitions(snapshotNamedSchemas, List.of(snapshotSchema));
        if (!snapshotSchema.equals(writerSchema)
                || !snapshotNamedSchemas.keySet().equals(namedSchemas.keySet())
                || namedSchemas.entrySet().stream().anyMatch(entry -> hasMetadataMismatch(entry.getValue(), snapshotNamedSchemas.get(entry.getKey())))) {
            throw new SchemaParseException("Avro writer schema snapshot differs from its source");
        }
        return new Snapshot(schemaJson, snapshotSchema);
    }

    static void mergeNamedDefinitions(Map<String, Schema> namedSchemas, Iterable<Schema> schemaRoots) {
        Set<Schema> visitedSchemas = Collections.newSetFromMap(new IdentityHashMap<>());
        Deque<Schema> pendingSchemas = new ArrayDeque<>();
        schemaRoots.forEach(pendingSchemas::push);
        while (!pendingSchemas.isEmpty()) {
            Schema schema = pendingSchemas.pop();
            if (!visitedSchemas.add(schema)) {
                continue;
            }
            switch (schema.getType()) {
                case RECORD -> {
                    mergeDefinition(namedSchemas, schema);
                    schema.getFields().forEach(field -> pendingSchemas.push(field.schema()));
                }
                case ENUM, FIXED -> mergeDefinition(namedSchemas, schema);
                case ARRAY -> pendingSchemas.push(schema.getElementType());
                case MAP -> pendingSchemas.push(schema.getValueType());
                case UNION -> pendingSchemas.addAll(schema.getTypes());
                default -> {
                }
            }
        }
    }

    static void mergeDefinition(Map<String, Schema> namedSchemas, Schema schema) {
        Schema existingSchema = namedSchemas.putIfAbsent(schema.getFullName(), schema);
        if (existingSchema != null) {
            requireSameDefinition(existingSchema, schema);
        }
    }

    private static void requireSameDefinition(Schema firstSchema, Schema secondSchema) {
        if (firstSchema != secondSchema && (!firstSchema.equals(secondSchema) || hasMetadataMismatch(firstSchema, secondSchema))) {
            throw new SchemaParseException("Conflicting Avro named definitions");
        }
    }

    private static boolean hasMetadataMismatch(Schema firstSchema, Schema secondSchema) {
        // Avro compares JSON properties without depending on member order, but omits some named definition metadata.
        if (!Objects.equals(firstSchema.getDoc(), secondSchema.getDoc()) || !firstSchema.getAliases().equals(secondSchema.getAliases())) {
            return true;
        }
        return switch (firstSchema.getType()) {
            case RECORD -> firstSchema.isError() != secondSchema.isError() || hasFieldMetadataMismatch(firstSchema.getFields(), secondSchema.getFields());
            case ENUM -> !Objects.equals(firstSchema.getEnumDefault(), secondSchema.getEnumDefault());
            default -> false;
        };
    }

    private static boolean hasFieldMetadataMismatch(List<Schema.Field> firstFields, List<Schema.Field> secondFields) {
        for (int index = 0; index < firstFields.size(); index++) {
            Schema.Field firstField = firstFields.get(index);
            Schema.Field secondField = secondFields.get(index);
            if (!Objects.equals(firstField.doc(), secondField.doc()) || !firstField.aliases().equals(secondField.aliases())) {
                return true;
            }
        }
        return false;
    }

    private static boolean isNamed(Schema schema) {
        return switch (schema.getType()) {
            case RECORD, ENUM, FIXED -> true;
            default -> false;
        };
    }

    private static String writeExplicitEmptyNamespaceNames(Schema writerSchema, String schemaJson) {
        StringWriter stringWriter = new StringWriter();
        try (
                JsonParser jsonParser = JSON_FACTORY.createParser(ObjectReadContext.empty(), schemaJson);
                JsonGenerator jsonGenerator = JSON_FACTORY.createGenerator(ObjectWriteContext.empty(), stringWriter)
        ) {
            jsonParser.nextToken();
            new SnapshotWriter(jsonParser, jsonGenerator).writeType(writerSchema, null);
        }
        return stringWriter.toString();
    }

    record Snapshot(String schemaJson, Schema schema) {
    }

    private record SnapshotWriter(JsonParser jsonParser, JsonGenerator jsonGenerator) {

        private void writeType(Schema schema, String namespace) {
            switch (this.jsonParser.currentToken()) {
                case VALUE_STRING -> {
                    if (namespace != null && isNamed(schema) && schema.getNamespace() == null) {
                        // A short name could bind in the enclosing namespace. Use Avro's empty-namespace alias spelling for references too.
                        this.jsonGenerator.writeString("." + schema.getName());
                    } else {
                        this.jsonGenerator.copyCurrentEvent(this.jsonParser);
                    }
                }
                case START_ARRAY -> {
                    this.jsonGenerator.writeStartArray();
                    for (Schema branchSchema : schema.getTypes()) {
                        this.jsonParser.nextToken();
                        this.writeType(branchSchema, namespace);
                    }
                    this.jsonParser.nextToken();
                    this.jsonGenerator.writeEndArray();
                }
                case START_OBJECT -> this.writeTypeObject(schema, namespace);
                default -> throw new SchemaParseException("Unexpected Avro schema JSON");
            }
        }

        private void writeTypeObject(Schema schema, String namespace) {
            this.jsonGenerator.writeStartObject();
            while (this.jsonParser.nextToken() == JsonToken.PROPERTY_NAME) {
                String propertyName = this.jsonParser.currentName();
                this.jsonGenerator.writeName(propertyName);
                this.jsonParser.nextToken();
                switch (propertyName) {
                    case "fields" -> this.writeFields(schema);
                    case "items" -> this.writeType(schema.getElementType(), namespace);
                    case "values" -> this.writeType(schema.getValueType(), namespace);
                    case "aliases" -> {
                        this.jsonGenerator.writeStartArray();
                        schema.getAliases().forEach(this.jsonGenerator::writeString);
                        this.jsonGenerator.writeEndArray();
                        this.jsonParser.skipChildren();
                    }
                    default -> this.jsonGenerator.copyCurrentStructure(this.jsonParser);
                }
            }
            this.jsonGenerator.writeEndObject();
        }

        private void writeFields(Schema schema) {
            this.jsonGenerator.writeStartArray();
            for (Schema.Field field : schema.getFields()) {
                this.jsonParser.nextToken();
                this.jsonGenerator.writeStartObject();
                while (this.jsonParser.nextToken() == JsonToken.PROPERTY_NAME) {
                    String propertyName = this.jsonParser.currentName();
                    this.jsonGenerator.writeName(propertyName);
                    this.jsonParser.nextToken();
                    if ("type".equals(propertyName)) {
                        this.writeType(field.schema(), schema.getNamespace());
                    } else {
                        this.jsonGenerator.copyCurrentStructure(this.jsonParser);
                    }
                }
                this.jsonGenerator.writeEndObject();
            }
            this.jsonParser.nextToken();
            this.jsonGenerator.writeEndArray();
        }
    }

    private static final class EnvelopeParseContext extends ParseContext {

        private final Map<String, Schema> referencedSchemas;

        private EnvelopeParseContext(Map<String, Schema> referencedSchemas) {
            this.referencedSchemas = referencedSchemas;
        }

        @Override
        public Schema find(String name, String namespace) {
            if (name.startsWith(".") && name.indexOf('.', 1) < 0) {
                Schema schema = super.find(name.substring(1), null);
                if (!isNamed(schema)) {
                    throw new SchemaParseException("Expected an empty-namespace Avro named type");
                }
                return schema;
            }
            return super.find(name, namespace);
        }

        @Override
        public Schema getNamedSchema(String fullName) {
            Schema schema = super.getNamedSchema(fullName);
            return schema == null ? this.referencedSchemas.get(fullName) : schema;
        }

        @Override
        public void put(Schema schema) {
            // Check every declaration before Avro keeps only the first definition of a name.
            Schema existingSchema = super.getNamedSchema(schema.getFullName());
            if (existingSchema == null) {
                super.put(schema);
            } else {
                requireSameDefinition(existingSchema, schema);
            }
        }

        @Override
        public void commit() {
            // Make references available to Avro's resolver without replacing the inline definitions.
            this.referencedSchemas.forEach((fullName, schema) -> {
                if (super.getNamedSchema(fullName) == null) {
                    super.put(schema);
                }
            });
            super.commit();
        }
    }
}
