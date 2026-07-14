package gregor.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

@Configuration(proxyBeanMethods = false)
public class JacksonConfiguration {

    @Bean
    @Primary
    public JsonMapper jsonMapper(JsonMapper.Builder builder) {
        return builder
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .changeDefaultPropertyInclusion(jiv -> jiv.withValueInclusion(JsonInclude.Include.NON_EMPTY))
                .build();
    }
}
