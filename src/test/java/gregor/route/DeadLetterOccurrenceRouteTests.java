package gregor.route;

import gregor.avro.ApicurioWebhookEnvelopeReader;
import gregor.avro.WebhookEnvelopeCodec;
import gregor.avro.WebhookEnvelopeFixtures;
import gregor.dto.StoredWebhookEnvelope;
import gregor.exception.InvalidWebhookEnvelopeException;
import gregor.repository.DeadLetterOccurrenceWriter;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.apache.camel.test.spring.junit5.UseAdviceWith;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@UseAdviceWith
@CamelSpringBootTest
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class DeadLetterOccurrenceRouteTests {

    private static final String TOPIC = "webhook-events-dead-letter";
    private static final String ROUTE_ID = "kafka:webhook-events-dead-letter?groupId=webhook-events-dead-letter";
    private static final String INPUT = "direct:dead-letter-occurrence-input";
    private static final long VALID_TIMESTAMP = 1_234L;
    private static final Instant DEAD_LETTERED_AT = Instant.parse("1970-01-01T00:00:01.234Z");
    private static final UUID EVENT_ID = UUID.fromString("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa");
    private static final int REPLAY_COUNT = 2;

    @Autowired
    private CamelContext camelContext;
    @Autowired
    private ProducerTemplate producerTemplate;
    @MockitoBean
    private ApicurioWebhookEnvelopeReader apicurioWebhookEnvelopeReader;
    @MockitoBean
    private DeadLetterOccurrenceWriter deadLetterOccurrenceWriter;

    private final KafkaManualCommit kafkaManualCommit = Mockito.mock(KafkaManualCommit.class);
    private final Headers kafkaHeaders = new RecordHeaders().add("apicurio.value.contentId", new byte[Long.BYTES]);
    private final byte[] wireBytes = {0, 0, 0, 0, 5, 10};
    private final StoredWebhookEnvelope storedWebhookEnvelope = new WebhookEnvelopeCodec().encode(WebhookEnvelopeFixtures.currentEvent(EVENT_ID.toString(), REPLAY_COUNT, null));

    @BeforeEach
    void startOnlyTheStorageRoute() throws Exception {
        AdviceWith.adviceWith(this.camelContext, ROUTE_ID, false, adviceWithRouteBuilder -> adviceWithRouteBuilder.replaceFromWith(INPUT));
        this.camelContext.start();
        this.camelContext.getRouteController().startRoute(ROUTE_ID);
    }

    @ParameterizedTest
    @CsvSource({"0, 1970-01-01T00:00:00Z", "1234, 1970-01-01T00:00:01.234Z"})
    void retainsUnconvertedKafkaInputAndCommitsAfterPersistence(long timestampMillis, Instant expectedDeadLetteredAt) {
        Map<String, Object> headers = this.headers();
        headers.put(KafkaConstants.TIMESTAMP, timestampMillis);
        Mockito.when(this.apicurioWebhookEnvelopeReader.read(TOPIC, this.kafkaHeaders, this.wireBytes)).thenReturn(this.storedWebhookEnvelope);
        Mockito.when(this.deadLetterOccurrenceWriter.insertIfAbsent(Mockito.any(UUID.class), Mockito.eq(EVENT_ID), Mockito.eq(REPLAY_COUNT), Mockito.same(this.storedWebhookEnvelope), Mockito.eq(expectedDeadLetteredAt))).thenReturn(1);

        this.producerTemplate.sendBodyAndHeaders(INPUT, this.wireBytes, headers);

        InOrder inOrder = Mockito.inOrder(this.apicurioWebhookEnvelopeReader, this.deadLetterOccurrenceWriter, this.kafkaManualCommit);
        inOrder.verify(this.apicurioWebhookEnvelopeReader).read(Mockito.eq(TOPIC), Mockito.same(this.kafkaHeaders), Mockito.same(this.wireBytes));
        inOrder.verify(this.deadLetterOccurrenceWriter).insertIfAbsent(Mockito.any(UUID.class), Mockito.eq(EVENT_ID), Mockito.eq(REPLAY_COUNT), Mockito.same(this.storedWebhookEnvelope), Mockito.eq(expectedDeadLetteredAt));
        inOrder.verify(this.kafkaManualCommit).commit();
        Mockito.verifyNoMoreInteractions(this.kafkaManualCommit);
    }

    @ParameterizedTest
    @EnumSource
    void rejectsInvalidMetadataBeforeReadingOrWriting(InvalidMetadata invalidMetadata) {
        Map<String, Object> headers = this.headers();
        switch (invalidMetadata) {
            case MISSING_TIMESTAMP -> headers.remove(KafkaConstants.TIMESTAMP);
            case NEGATIVE_TIMESTAMP -> headers.put(KafkaConstants.TIMESTAMP, -1L);
            case STRING_TIMESTAMP -> headers.put(KafkaConstants.TIMESTAMP, "1234");
            case INTEGER_TIMESTAMP -> headers.put(KafkaConstants.TIMESTAMP, 1234);
            case MISSING_COMMIT -> headers.remove(KafkaConstants.MANUAL_COMMIT);
            case WRONG_COMMIT_TYPE -> headers.put(KafkaConstants.MANUAL_COMMIT, "manual-commit");
            case MISSING_TIMESTAMP_AND_COMMIT -> {
                headers.remove(KafkaConstants.TIMESTAMP);
                headers.remove(KafkaConstants.MANUAL_COMMIT);
            }
        }

        Throwable throwable = Assertions.catchThrowable(() -> this.producerTemplate.sendBodyAndHeaders(INPUT, this.wireBytes, headers));

        switch (invalidMetadata) {
            case MISSING_COMMIT, WRONG_COMMIT_TYPE -> Assertions.assertThat(throwable).cause().isExactlyInstanceOf(IllegalStateException.class).hasMessage("Dead-letter Kafka manual commit is missing").hasNoCause();
            default -> Assertions.assertThat(throwable).cause().isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("Dead-letter Kafka record timestamp must be nonnegative epoch milliseconds").hasNoCause();
        }
        Mockito.verifyNoInteractions(this.apicurioWebhookEnvelopeReader, this.deadLetterOccurrenceWriter, this.kafkaManualCommit);
    }

    @Test
    void leavesReaderFailureUnhandledWithoutWritingOrCommitting() {
        InvalidWebhookEnvelopeException invalidWebhookEnvelopeException = new InvalidWebhookEnvelopeException("Webhook envelope writer schema is incompatible");
        Mockito.when(this.apicurioWebhookEnvelopeReader.read(TOPIC, this.kafkaHeaders, this.wireBytes)).thenThrow(invalidWebhookEnvelopeException);

        Assertions.assertThatThrownBy(() -> this.producerTemplate.sendBodyAndHeaders(INPUT, this.wireBytes, this.headers())).hasRootCause(invalidWebhookEnvelopeException);
        Mockito.verifyNoInteractions(this.deadLetterOccurrenceWriter, this.kafkaManualCommit);
    }

    @Test
    void leavesPersistenceFailureUnhandledWithoutCommitting() {
        Mockito.when(this.apicurioWebhookEnvelopeReader.read(TOPIC, this.kafkaHeaders, this.wireBytes)).thenReturn(this.storedWebhookEnvelope);
        Mockito.when(this.deadLetterOccurrenceWriter.insertIfAbsent(Mockito.any(UUID.class), Mockito.eq(EVENT_ID), Mockito.eq(REPLAY_COUNT), Mockito.same(this.storedWebhookEnvelope), Mockito.eq(DEAD_LETTERED_AT)))
                .thenThrow(new IllegalStateException("private transaction detail", new SQLException("private driver detail", "23514")));

        Assertions.assertThatThrownBy(() -> this.producerTemplate.sendBodyAndHeaders(INPUT, this.wireBytes, this.headers()))
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("Dead-letter occurrence transaction failed [sqlState=23514]");
        Mockito.verifyNoInteractions(this.kafkaManualCommit);
    }

    private Map<String, Object> headers() {
        return new HashMap<>(Map.of(KafkaConstants.TOPIC, TOPIC, KafkaConstants.HEADERS, this.kafkaHeaders, KafkaConstants.TIMESTAMP, VALID_TIMESTAMP, KafkaConstants.MANUAL_COMMIT, this.kafkaManualCommit));
    }

    private enum InvalidMetadata {
        MISSING_TIMESTAMP, NEGATIVE_TIMESTAMP, STRING_TIMESTAMP, INTEGER_TIMESTAMP, MISSING_COMMIT, WRONG_COMMIT_TYPE, MISSING_TIMESTAMP_AND_COMMIT
    }
}
