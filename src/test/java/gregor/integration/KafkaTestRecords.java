package gregor.integration;

import gregor.avro.DeliveryAttempt;
import gregor.avro.WebhookEvent;
import gregor.route.WebhookRoute;
import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.vertx.core.Vertx;
import lombok.NoArgsConstructor;
import org.apache.camel.spi.RouteController;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Assertions;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@NoArgsConstructor
final class KafkaTestRecords {

    private static final String[] RETRY_TOPICS = {WebhookRoute.WEBHOOK_EVENT_RETRY_1M_TOPIC, WebhookRoute.WEBHOOK_EVENT_RETRY_10M_TOPIC, WebhookRoute.WEBHOOK_EVENT_RETRY_1H_TOPIC};

    static AdminClient adminClient() {
        return AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers()));
    }

    static KafkaProducer<String, byte[]> rawProducer() {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.ACKS_CONFIG, "all"
        ));
    }

    static Map<TopicPartition, Long> endOffsets(AdminClient adminClient, String... topics) throws Exception {
        Map<TopicPartition, OffsetSpec> partitions = adminClient.describeTopics(List.of(topics))
                .allTopicNames().get(5, TimeUnit.SECONDS).values().stream()
                .flatMap(topicDescription -> topicDescription.partitions().stream().map(topicPartitionInfo -> new TopicPartition(topicDescription.name(), topicPartitionInfo.partition())))
                .collect(Collectors.toMap(topicPartition -> topicPartition, _ -> OffsetSpec.latest()));
        return adminClient.listOffsets(partitions).all().get(5, TimeUnit.SECONDS).entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().offset()));
    }

    static Map<TopicPartition, Long> retryEndOffsets(AdminClient adminClient) throws Exception {
        return endOffsets(adminClient, RETRY_TOPICS);
    }

    static void drainRetries(AdminClient adminClient, RouteController routeController, Map<TopicPartition, Long> offsetsBefore, AtomicReference<Instant> currentInstantReference) throws Exception {
        Map<TopicPartition, Long> pendingOffsets = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : retryEndOffsets(adminClient).entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            long endOffset = entry.getValue();
            if (endOffset > offsetsBefore.get(topicPartition) && committedOffset(adminClient, topicPartition) < endOffset) {
                pendingOffsets.put(topicPartition, endOffset);
            }
        }
        if (pendingOffsets.isEmpty()) {
            return;
        }

        List<String> routes = pendingOffsets.keySet().stream().map(topicPartition -> consumerRoute(topicPartition.topic())).distinct().toList();
        // A retry consumer already waiting will not recalculate its delay until it restarts.
        for (String route : routes) {
            if (!routeController.getRouteStatus(route).isStopped()) {
                stopRetryConsumer(routeController, route);
            }
        }
        currentInstantReference.updateAndGet(instant -> instant.plus(Duration.ofDays(1)));
        for (String route : routes) {
            routeController.startRoute(route);
        }
        for (Map.Entry<TopicPartition, Long> entry : pendingOffsets.entrySet()) {
            awaitCommit(adminClient, entry.getKey(), entry.getValue() - 1);
        }
    }

    static String consumerRoute(String topic) {
        return "kafka:" + topic + "?groupId=" + topic;
    }

    static void stopRetryConsumer(RouteController routeController, String routeId) throws Exception {
        Assertions.assertThat(routeController.stopRoute(routeId, 1, TimeUnit.SECONDS, false)).as("retry consumer %s is stopped", routeId).isTrue();
    }

    static KafkaConsumer<String, Object> consumer(Vertx registryVertx, Map<TopicPartition, Long> offsets) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "gregor-test-observer-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.apicurio.registry.serde.avro.AvroKafkaDeserializer");
        properties.put("apicurio.registry.url", "http://localhost:" + System.getenv().getOrDefault("SCHEMA_REGISTRY_PORT", "8081") + "/apis/registry/v3");
        properties.put("apicurio.registry.use-specific-avro-reader", true);
        properties.put(SchemaResolverConfig.VERTX_INSTANCE, registryVertx);
        KafkaConsumer<String, Object> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.assign(offsets.keySet());
        offsets.forEach(kafkaConsumer::seek);
        return kafkaConsumer;
    }

    static ConsumerRecord<String, Object> awaitRecord(KafkaConsumer<String, Object> kafkaConsumer, String eventId) {
        Instant deadline = Instant.now().plusSeconds(20);
        while (Instant.now().isBefore(deadline)) {
            for (ConsumerRecord<String, Object> consumerRecord : kafkaConsumer.poll(Duration.ofMillis(200))) {
                if (matchesEvent(consumerRecord.value(), eventId)) {
                    return consumerRecord;
                }
            }
        }
        throw new AssertionError("Kafka record was not observed for event " + eventId);
    }

    static long committedOffset(AdminClient adminClient, ConsumerRecord<String, Object> inputConsumerRecord) throws Exception {
        return committedOffset(adminClient, new TopicPartition(inputConsumerRecord.topic(), inputConsumerRecord.partition()));
    }

    static void awaitCommit(AdminClient adminClient, ConsumerRecord<String, Object> inputConsumerRecord) throws Exception {
        awaitCommit(adminClient, new TopicPartition(inputConsumerRecord.topic(), inputConsumerRecord.partition()), inputConsumerRecord.offset());
    }

    static void awaitPublishedCommits(AdminClient adminClient, String topic, Map<TopicPartition, Long> offsetsBefore) throws Exception {
        for (Map.Entry<TopicPartition, Long> entry : endOffsets(adminClient, topic).entrySet()) {
            if (entry.getValue() > offsetsBefore.get(entry.getKey())) {
                awaitCommit(adminClient, entry.getKey(), entry.getValue() - 1);
            }
        }
    }

    static void awaitCommit(AdminClient adminClient, TopicPartition topicPartition, long recordOffset) throws Exception {
        Instant deadline = Instant.now().plusSeconds(20);
        while (Instant.now().isBefore(deadline)) {
            if (committedOffset(adminClient, topicPartition) > recordOffset) {
                return;
            }
            Thread.sleep(50L);
        }
        throw new AssertionError("Offset " + recordOffset + " was not committed on " + topicPartition);
    }

    private static long committedOffset(AdminClient adminClient, TopicPartition topicPartition) throws Exception {
        return committedOffset(adminClient, topicPartition.topic(), topicPartition);
    }

    static long committedOffset(AdminClient adminClient, String groupId, TopicPartition topicPartition) throws Exception {
        OffsetAndMetadata offsetAndMetadata = adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS).get(topicPartition);
        return Objects.isNull(offsetAndMetadata) ? -1L : offsetAndMetadata.offset();
    }

    private static boolean matchesEvent(Object recordValue, String eventId) {
        return switch (recordValue) {
            case WebhookEvent webhookEvent -> Objects.equals(eventId, webhookEvent.getEventId());
            case DeliveryAttempt deliveryAttempt -> Objects.equals(eventId, deliveryAttempt.getEventId());
            default -> false;
        };
    }

    private static String brokers() {
        return "localhost:" + System.getenv().getOrDefault("KAFKA_PORT", "9092");
    }
}
