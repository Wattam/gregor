package gregor.config;

import org.jspecify.annotations.NonNull;
import org.springframework.boot.context.properties.ConfigurationPropertiesBindHandlerAdvisor;
import org.springframework.boot.context.properties.bind.AbstractBindHandler;
import org.springframework.boot.context.properties.bind.BindContext;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.PropertySourcesPlaceholdersResolver;
import org.springframework.boot.context.properties.source.ConfigurationProperty;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;
import org.springframework.boot.origin.OriginLookup;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertySource;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;

@Configuration(proxyBeanMethods = false)
class DeadLetterBatchSizeBindingConfiguration {

    @Bean
    static ConfigurationPropertiesBindHandlerAdvisor deadLetterBatchSizeBindHandlerAdvisor(ConfigurableEnvironment configurableEnvironment) {
        return bindHandler -> new AbstractBindHandler(bindHandler) {
            @Override
            public Object onSuccess(@NonNull ConfigurationPropertyName name, @NonNull Bindable<?> target, @NonNull BindContext context, @NonNull Object result) {
                ConfigurationProperty configurationProperty = context.getConfigurationProperty();
                if (Objects.equals(name, DeadLetterBatchSizeYamlPropertySourceLoader.BATCH_SIZE_PROPERTY) && Objects.nonNull(configurationProperty)) {
                    Object value = DeadLetterBatchSizeYamlPropertySourceLoader.scalarValue(configurationProperty.getValue(), configurationProperty.getOrigin());
                    if (value instanceof String scalarValue) {
                        result = new ScalarPlaceholdersResolver(configurableEnvironment).resolve(scalarValue);
                    }
                }
                return super.onSuccess(name, target, context, result);
            }
        };
    }

    private static final class ScalarPlaceholdersResolver extends PropertySourcesPlaceholdersResolver {

        private final ConfigurableEnvironment configurableEnvironment;
        private final String tokenPrefix = UUID.randomUUID().toString();
        private final Map<ScalarValue, String> tokens = new HashMap<>();

        private ScalarPlaceholdersResolver(ConfigurableEnvironment configurableEnvironment) {
            super(configurableEnvironment);
            this.configurableEnvironment = configurableEnvironment;
        }

        private String resolve(String batchSize) {
            return this.replaceTokens((String) this.resolvePlaceholders(batchSize), ScalarValue::scalarText);
        }

        @Override
        protected String resolvePlaceholder(@NonNull String placeholder) {
            String name = this.replaceTokens(placeholder, ScalarValue::typedText);
            for (PropertySource<?> propertySource : this.configurableEnvironment.getPropertySources()) {
                Object value = propertySource.getProperty(name);
                if (Objects.nonNull(value)) {
                    String typedText = String.valueOf(value);
                    String scalarText = String.valueOf(DeadLetterBatchSizeYamlPropertySourceLoader.scalarValue(value, OriginLookup.getOrigin(propertySource, name)));
                    if (Objects.equals(typedText, scalarText)) {
                        return typedText;
                    }
                    // Defer the choice: nested keys use Boot's typed text, while the final value uses the YAML scalar text.
                    return this.tokens.computeIfAbsent(new ScalarValue(typedText, scalarText), _ -> "[[" + this.tokenPrefix + ":" + this.tokens.size() + "]]");
                }
            }
            return null;
        }

        private String replaceTokens(String text, Function<ScalarValue, String> replacement) {
            for (Map.Entry<ScalarValue, String> entry : this.tokens.entrySet()) {
                text = text.replace(entry.getValue(), replacement.apply(entry.getKey()));
            }
            return text;
        }
    }

    private record ScalarValue(String typedText, String scalarText) {
    }
}
