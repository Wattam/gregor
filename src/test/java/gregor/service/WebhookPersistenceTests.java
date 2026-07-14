package gregor.service;

import gregor.avro.DeliveryAttempt;
import gregor.avro.DeliveryOutcome;
import gregor.component.RestComponent;
import gregor.config.WebhookPropertiesFixtures;
import gregor.repository.DeliveryAttemptRepository;
import org.apache.camel.ProducerTemplate;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

class WebhookPersistenceTests {

    @Test
    void persistenceBoundaryIsTransactional() throws ReflectiveOperationException {
        Transactional transactional = WebhookService.class
                .getDeclaredMethod("persistDeliveryAttempt", DeliveryAttempt.class)
                .getAnnotation(Transactional.class);

        Assertions.assertThat(transactional).isNotNull();
    }

    @Test
    void convertsEveryEnvelopeValueAndInvokesTheTypedInsert() {
        AtomicReference<InvocationOnMock> insertInvocation = new AtomicReference<>();
        DeliveryAttemptRepository deliveryAttemptRepository = Mockito.mock(
                DeliveryAttemptRepository.class,
                invocation -> {
                    if (invocation.getMethod().getDeclaringClass() == DeliveryAttemptRepository.class) {
                        insertInvocation.set(invocation);
                        return 1;
                    }
                    return Answers.RETURNS_DEFAULTS.answer(invocation);
                }
        );
        ObjectMapper objectMapper = new ObjectMapper();
        WebhookService webhookService = new WebhookService(
                Mockito.mock(RestComponent.class),
                deliveryAttemptRepository,
                objectMapper,
                WebhookPropertiesFixtures.create(),
                Mockito.mock(ProducerTemplate.class),
                Clock.systemUTC()
        );
        DeliveryAttempt deliveryAttempt = new DeliveryAttempt(
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "cccccccc-cccc-cccc-cccc-cccccccccccc",
                "dddddddd-dddd-dddd-dddd-dddddddddddd",
                "order.created",
                3,
                DeliveryOutcome.RETRYABLE_FAILURE,
                503,
                42L,
                Instant.parse("2026-07-10T12:00:00Z"),
                ByteBuffer.wrap("{\"type\":\"HTTP_FAILURE\"}".getBytes(StandardCharsets.UTF_8))
        );

        webhookService.persistDeliveryAttempt(deliveryAttempt);

        Assertions.assertThat(insertInvocation.get()).isNotNull();
        Object[] arguments = insertInvocation.get().getArguments();
        Assertions.assertThat(arguments).hasSize(11);
        Assertions.assertThat(arguments[0]).isEqualTo(UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"));
        Assertions.assertThat(arguments[1]).isEqualTo(UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"));
        Assertions.assertThat(arguments[2]).isEqualTo(UUID.fromString("cccccccc-cccc-cccc-cccc-cccccccccccc"));
        Assertions.assertThat(arguments[3]).isEqualTo(UUID.fromString("dddddddd-dddd-dddd-dddd-dddddddddddd"));
        Assertions.assertThat(arguments[4]).isEqualTo("order.created");
        Assertions.assertThat(arguments[5]).isEqualTo(3);
        Assertions.assertThat(arguments[6]).isEqualTo("RETRYABLE_FAILURE");
        Assertions.assertThat(arguments[7]).isEqualTo(503);
        Assertions.assertThat(arguments[8]).isEqualTo(42L);
        Assertions.assertThat(arguments[9]).isEqualTo(Instant.parse("2026-07-10T12:00:00Z"));
        JsonNode details = objectMapper.readTree((String) arguments[10]);
        Assertions.assertThat(details.get("type").asString()).isEqualTo("HTTP_FAILURE");
    }

    @Test
    void preservesNullHttpStatusAndTreatsConflictCountZeroAsSuccessfulPersistence() {
        AtomicReference<InvocationOnMock> insertInvocation = new AtomicReference<>();
        DeliveryAttemptRepository deliveryAttemptRepository = Mockito.mock(
                DeliveryAttemptRepository.class,
                invocation -> {
                    if (invocation.getMethod().getDeclaringClass() == DeliveryAttemptRepository.class) {
                        insertInvocation.set(invocation);
                        return 0;
                    }
                    return Answers.RETURNS_DEFAULTS.answer(invocation);
                }
        );
        WebhookService webhookService = new WebhookService(
                Mockito.mock(RestComponent.class),
                deliveryAttemptRepository,
                new ObjectMapper(),
                WebhookPropertiesFixtures.create(),
                Mockito.mock(ProducerTemplate.class),
                Clock.systemUTC()
        );
        DeliveryAttempt deliveryAttempt = new DeliveryAttempt(
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "cccccccc-cccc-cccc-cccc-cccccccccccc",
                "dddddddd-dddd-dddd-dddd-dddddddddddd",
                "order.created",
                1,
                DeliveryOutcome.SUCCEEDED,
                null,
                12L,
                Instant.EPOCH,
                ByteBuffer.wrap("{}".getBytes(StandardCharsets.UTF_8))
        );

        Assertions.assertThatCode(() -> webhookService.persistDeliveryAttempt(deliveryAttempt))
                .doesNotThrowAnyException();

        Assertions.assertThat(insertInvocation.get()).isNotNull();
        Assertions.assertThat(insertInvocation.get().getArguments()[7]).isNull();
    }
}
