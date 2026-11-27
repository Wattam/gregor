package gregor.config;

import lombok.Getter;
import org.apache.commons.lang3.Strings;
import org.jspecify.annotations.NullMarked;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;
import org.springframework.boot.env.OriginTrackedMapPropertySource;
import org.springframework.boot.env.PropertiesPropertySourceLoader;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.origin.Origin;
import org.springframework.boot.origin.OriginTrackedValue;
import org.springframework.boot.origin.PropertySourceOrigin;
import org.springframework.boot.origin.TextResourceOrigin;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.Mark;
import org.yaml.snakeyaml.events.Event;
import org.yaml.snakeyaml.events.ScalarEvent;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.reader.UnicodeReader;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Order(Ordered.HIGHEST_PRECEDENCE + 1)
public class DeadLetterBatchSizeYamlPropertySourceLoader extends YamlPropertySourceLoader {

    static final ConfigurationPropertyName BATCH_SIZE_PROPERTY = ConfigurationPropertyName.of("gregor.dead-letter.cleanup-batch-size");

    @Override
    @NullMarked
    public List<PropertySource<?>> load(String name, Resource resource) throws IOException {
        Resource snapshot = new ByteArrayResource(resource.getContentAsByteArray(), resource.getDescription());
        List<PropertySource<?>> propertySources = super.load(name, snapshot);
        Map<TextResourceOrigin.Location, String> scalarValues = readScalarValues(snapshot);
        return propertySources.stream().<PropertySource<?>>map(propertySource -> trackScalars((OriginTrackedMapPropertySource) propertySource, resource, scalarValues)).toList();
    }

    private static PropertySource<?> trackScalars(OriginTrackedMapPropertySource propertySource, Resource resource, Map<TextResourceOrigin.Location, String> scalarValues) {
        Map<String, Object> entries = new LinkedHashMap<>(propertySource.getSource());
        markMalformedBatchSize(entries);
        entries.replaceAll((_, entry) -> {
            if (!(entry instanceof OriginTrackedValue originTrackedValue) || !(originTrackedValue.getOrigin() instanceof TextResourceOrigin textResourceOrigin)) {
                return entry;
            }
            TextResourceOrigin.Location location = textResourceOrigin.getLocation();
            Object value = originTrackedValue.getValue();
            String scalarValue = scalarValues.get(location);
            // Keep Boot's typed value for normal lookups. Attach the original scalar only where coercion changed its type.
            TextResourceOrigin origin = !(value instanceof String) && Objects.nonNull(scalarValue) ? new ScalarOrigin(resource, location, scalarValue) : new TextResourceOrigin(resource, location);
            return OriginTrackedValue.of(value, origin);
        });
        return new OriginTrackedMapPropertySource(propertySource.getName(), Collections.unmodifiableMap(entries), true);
    }

    private static void markMalformedBatchSize(Map<String, Object> entries) {
        Map.Entry<String, Object> descendant = null;
        for (Map.Entry<String, Object> entry : entries.entrySet()) {
            String entryName = entry.getKey();
            ConfigurationPropertyName name = ConfigurationPropertyName.adapt(entryName, '.');
            boolean matchesBatchSize = Objects.equals(name, BATCH_SIZE_PROPERTY);
            // A child with an empty or punctuation-only name can disappear during relaxed name adaptation.
            if (matchesBatchSize && Strings.CS.endsWith(entryName, name.getLastElement(ConfigurationPropertyName.Form.ORIGINAL))) {
                return;
            }
            if (Objects.isNull(descendant) && (matchesBatchSize || BATCH_SIZE_PROPERTY.isAncestorOf(name))) {
                descendant = entry;
            }
        }
        if (Objects.nonNull(descendant)) {
            // Flattening keeps the children but drops their parent. Bind an invalid scalar instead of falling through to a lower source.
            entries.put(BATCH_SIZE_PROPERTY.toString(), OriginTrackedValue.of("", Origin.from(descendant.getValue())));
        }
    }

    private static Map<TextResourceOrigin.Location, String> readScalarValues(Resource snapshot) throws IOException {
        LoaderOptions loaderOptions = new LoaderOptions();
        loaderOptions.setCodePointLimit(Integer.MAX_VALUE);
        Map<TextResourceOrigin.Location, String> scalarValues = new HashMap<>();
        try (UnicodeReader unicodeReader = new UnicodeReader(snapshot.getInputStream())) {
            for (Event event : new Yaml(loaderOptions).parse(unicodeReader)) {
                if (event instanceof ScalarEvent scalarEvent) {
                    Mark mark = scalarEvent.getStartMark();
                    String tag = scalarEvent.getTag();
                    // A non-integer tag is significant even when the scalar contains only digits.
                    String scalarValue = Objects.isNull(tag) || Objects.equals(tag, "!") || Objects.equals(tag, Tag.INT.getValue()) ? scalarEvent.getValue() : tag + " " + scalarEvent.getValue();
                    scalarValues.put(new TextResourceOrigin.Location(mark.getLine(), mark.getColumn()), scalarValue);
                }
            }
        }
        return scalarValues;
    }

    static Object scalarValue(Object value, Origin origin) {
        Origin valueOrigin = origin instanceof PropertySourceOrigin propertySourceOrigin ? propertySourceOrigin.getOrigin() : origin;
        return valueOrigin instanceof ScalarOrigin scalarOrigin ? scalarOrigin.getScalarValue() : value;
    }

    private static final class ScalarOrigin extends TextResourceOrigin {

        @Getter
        private final String scalarValue;

        private ScalarOrigin(Resource resource, Location location, String scalarValue) {
            super(resource, location);
            this.scalarValue = scalarValue;
        }
    }

    // Config data discovers directory entries in loader order. Keep Boot's properties formats ahead of YAML.
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public static class PropertiesLoader extends PropertiesPropertySourceLoader {
    }
}
