package gregor.config;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;
import java.time.Duration;

class RestClientConfigurationTests {

    @Test
    void configuresJdkTransportTimeoutsAndNeverFollowsRedirects() {
        RestClientConfiguration restClientConfiguration = new RestClientConfiguration();
        RestClient restClient = restClientConfiguration.restClient(
                RestClient.builder(),
                WebhookPropertiesFixtures.create()
        );

        Object requestFactory = ReflectionTestUtils.getField(restClient, "clientRequestFactory");
        Assertions.assertThat(requestFactory).isInstanceOf(JdkClientHttpRequestFactory.class);
        Assertions.assertThat(requestFactory).isNotNull();
        HttpClient httpClient = (HttpClient) ReflectionTestUtils.getField(requestFactory, "httpClient");
        Duration readTimeout = (Duration) ReflectionTestUtils.getField(requestFactory, "readTimeout");
        Assertions.assertThat(httpClient).isNotNull();
        Assertions.assertThat(httpClient.connectTimeout()).contains(Duration.ofSeconds(2));
        Assertions.assertThat(httpClient.followRedirects()).isEqualTo(HttpClient.Redirect.NEVER);
        Assertions.assertThat(readTimeout).isEqualTo(Duration.ofSeconds(5));
    }
}
