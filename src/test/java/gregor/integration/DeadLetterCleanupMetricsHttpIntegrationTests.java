package gregor.integration;

import gregor.Application;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class DeadLetterCleanupMetricsHttpIntegrationTests {

    private static final int MANAGEMENT_PORT = 8082;
    private static final String METRICS_PATH = "/actuator/metrics";
    private static final String RETAINED = "gregor.deadletter.occurrences.retained";
    private static final String DELETED = "gregor.deadletter.cleanup.deleted";
    private static final String RUNS = "gregor.deadletter.cleanup.runs";

    @Test
    void exposesNoActuatorEndpointsWithProductionDefaults() throws Exception {
        try (ConfigurableApplicationContext configurableApplicationContext = startApplication(Map.of()); HttpClient httpClient = HttpClient.newHttpClient()) {
            int applicationPort = configurableApplicationContext.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
            int managementPort = configurableApplicationContext.getEnvironment().getProperty("local.management.port", Integer.class, applicationPort);
            Assertions.assertThat(managementPort).isEqualTo(applicationPort);
            assertManagementEndpointsNotFound(httpClient, applicationPort);
        }
    }

    @Test
    void documentedLocalOptInServesOnlyMetricsOnTheSeparateLoopbackPort() throws Exception {
        Map<String, Object> managementEnvironmentVariables = Map.of(
                "MANAGEMENT_SERVER_PORT", "8082",
                "MANAGEMENT_SERVER_ADDRESS", "127.0.0.1",
                "MANAGEMENT_ENDPOINT_METRICS_ACCESS", "read-only",
                "MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE", "metrics"
        );
        try (ConfigurableApplicationContext configurableApplicationContext = startApplication(managementEnvironmentVariables); HttpClient httpClient = HttpClient.newHttpClient()) {
            int applicationPort = configurableApplicationContext.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
            ObjectMapper objectMapper = configurableApplicationContext.getBean(ObjectMapper.class);
            assertManagementEndpointsNotFound(httpClient, applicationPort);
            assertNotFound(httpClient, MANAGEMENT_PORT, "/actuator", "/actuator/health", "/actuator/info");

            HttpResponse<String> metricsResponse = get(httpClient, MANAGEMENT_PORT, METRICS_PATH);
            Assertions.assertThat(metricsResponse.statusCode()).isEqualTo(200);
            List<String> meterNames = new ArrayList<>();
            objectMapper.readTree(metricsResponse.body()).get("names").forEach(name -> meterNames.add(name.asString()));
            Assertions.assertThat(meterNames).contains(RETAINED, DELETED, RUNS);
            Assertions.assertThat(send(httpClient, MANAGEMENT_PORT, METRICS_PATH, "POST").statusCode()).isIn(404, 405);

            JsonNode retainedMeasurement = readMetric(httpClient, objectMapper, RETAINED, "").get("measurements").get(0);
            Assertions.assertThat(retainedMeasurement.get("statistic").asString()).isEqualTo("VALUE");
            Assertions.assertThat(retainedMeasurement.path("value").isNumber()).isFalse();
            Assertions.assertThat(readMetric(httpClient, objectMapper, DELETED, "").get("measurements").get(0).get("value").asDouble()).isZero();
            Assertions.assertThat(readMetric(httpClient, objectMapper, RUNS, "?tag=outcome:success").get("measurements").get(0).get("value").asDouble()).isZero();
            Assertions.assertThat(readMetric(httpClient, objectMapper, RUNS, "?tag=outcome:failure").get("measurements").get(0).get("value").asDouble()).isZero();
            Assertions.assertThat(get(httpClient, MANAGEMENT_PORT, METRICS_PATH + "/" + RUNS + "?tag=outcome:unknown").statusCode()).isEqualTo(404);
            try (Socket socket = new Socket()) {
                Assertions.assertThatExceptionOfType(ConnectException.class).isThrownBy(() -> socket.connect(new InetSocketAddress("127.0.0.2", MANAGEMENT_PORT), 1_000));
            }
        }
    }

    private static ConfigurableApplicationContext startApplication(Map<String, Object> managementEnvironmentVariables) {
        SystemEnvironmentPropertySource systemEnvironmentPropertySource = new SystemEnvironmentPropertySource("test-management-systemEnvironment", managementEnvironmentVariables);
        return new SpringApplicationBuilder(Application.class)
                .web(WebApplicationType.SERVLET)
                .initializers(applicationContext -> applicationContext.getEnvironment().getPropertySources().addFirst(systemEnvironmentPropertySource))
                .run(
                        "--spring.config.location=file:./src/main/resources/application.yaml",
                        "--spring.datasource.url=jdbc:h2:mem:cleanup-metrics-http",
                        "--spring.datasource.username=sa",
                        "--spring.datasource.password=",
                        "--spring.jpa.hibernate.ddl-auto=none",
                        "--spring.kafka.admin.auto-create=false",
                        "--camel.main.auto-startup=false",
                        "--gregor.dead-letter.cleanup-interval=1d",
                        "--server.port=0",
                        "--server.address=127.0.0.1"
                );
    }

    private static void assertManagementEndpointsNotFound(HttpClient httpClient, int port) throws IOException, InterruptedException {
        assertNotFound(httpClient, port, "/actuator", "/actuator/health", "/actuator/info", METRICS_PATH, METRICS_PATH + "/" + RETAINED, METRICS_PATH + "/" + DELETED, METRICS_PATH + "/" + RUNS);
    }

    private static void assertNotFound(HttpClient httpClient, int port, String... paths) throws IOException, InterruptedException {
        for (String path : paths) {
            Assertions.assertThat(get(httpClient, port, path).statusCode()).as("GET %s on port %s", path, port).isEqualTo(404);
        }
    }

    private static JsonNode readMetric(HttpClient httpClient, ObjectMapper objectMapper, String meterName, String query) throws IOException, InterruptedException {
        HttpResponse<String> httpResponse = get(httpClient, MANAGEMENT_PORT, METRICS_PATH + "/" + meterName + query);
        Assertions.assertThat(httpResponse.statusCode()).as("GET metric %s%s", meterName, query).isEqualTo(200);
        JsonNode metricJsonNode = objectMapper.readTree(httpResponse.body());
        Assertions.assertThat(metricJsonNode.get("name").asString()).isEqualTo(meterName);
        return metricJsonNode;
    }

    private static HttpResponse<String> get(HttpClient httpClient, int port, String path) throws IOException, InterruptedException {
        return send(httpClient, port, path, "GET");
    }

    private static HttpResponse<String> send(HttpClient httpClient, int port, String path, String method) throws IOException, InterruptedException {
        HttpRequest httpRequest = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).timeout(Duration.ofSeconds(5)).method(method, HttpRequest.BodyPublishers.noBody()).build();
        return httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
    }
}
