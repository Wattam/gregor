package gregor.config;

import gregor.config.properties.WebhookProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;

@Configuration(proxyBeanMethods = false)
public class RestClientConfiguration {

    @Bean
    public RestClient restClient(RestClient.Builder restClientBuilder, WebhookProperties webhookProperties) {
        HttpClient httpClient = HttpClient.newBuilder().connectTimeout(webhookProperties.connectionTimeout()).followRedirects(HttpClient.Redirect.NEVER).build();
        JdkClientHttpRequestFactory jdkClientHttpRequestFactory = new JdkClientHttpRequestFactory(httpClient);
        jdkClientHttpRequestFactory.setReadTimeout(webhookProperties.responseTimeout());
        return restClientBuilder.requestFactory(jdkClientHttpRequestFactory).build();
    }
}
