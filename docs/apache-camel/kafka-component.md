# Apache Camel Kafka component

This file contains the Kafka component rules and options that apply to Gregor.
It applies to `camel-kafka-starter` 4.21.0.

Upstream source: https://camel.apache.org/components/next/kafka-component.html

## URI and role

Use this URI form:

```text
kafka:topicName?option=value
```

A route with `from("kafka:...")` is a consumer. A route with `to("kafka:...")` is a producer.

```java
from("kafka:webhook-events?groupId=webhook-events")
    .to("direct:webhook-delivery-attempt");

from("direct:publish-attempt")
    .to("kafka:persist-delivery-attempt");
```

Set shared options under `camel.component.kafka` in `application.yaml`.
Set topic and route-specific options in the URI.

## Spring Boot configuration

Camel 4.21 bridges supported `spring.kafka.*` values into the Camel Kafka component.
The bridge includes broker, client ID, consumer group, security protocol, TLS, and SASL settings.
An explicit `camel.component.kafka.*` value takes precedence over a bridged Spring value.

Gregor uses `spring.kafka` for Spring Kafka administration and `camel.component.kafka` for Camel routes.
Keep explicit Camel settings when route behavior depends on them.

Disable the bridge only when both clients require different supported values:

```yaml
camel:
    component:
        kafka:
            bridge-spring-kafka-properties: false
```

## Consumer groups and concurrency

Set `groupId` on every consumer endpoint. Consumers with the same group ID share topic partitions.
Consumers with different group IDs each receive the records independently.

`consumersCount` creates that number of Kafka consumers. Each consumer has its own thread.
Useful concurrency cannot exceed the partitions assigned to the consumer group.

A Kafka record key selects a partition through the configured partitioner.
Records with the same key stay ordered in one topic partition.
This ordering does not apply across topics or across a republish boundary.

## Serialization

Gregor uses these component options:

```yaml
camel:
    component:
        kafka:
            key-serializer: org.apache.kafka.common.serialization.StringSerializer
            value-serializer: io.apicurio.registry.serde.avro.AvroKafkaSerializer
            key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
            value-deserializer: io.apicurio.registry.serde.avro.AvroKafkaDeserializer
            additional-properties:
                "[apicurio.registry.url]": ${APICURIO_REGISTRY_URL}
                "[apicurio.registry.use-specific-avro-reader]": true
                "[apicurio.registry.auto-register]": true
```

The key serializer and deserializer must support the Java key type.
The value serializer and deserializer must support the Avro record type and registry protocol.

Use `additionalProperties` for Kafka or serializer properties that Camel does not expose as direct options.
In Spring configuration, use bracketed map keys for dotted property names.

## Record keys and partitions

Set a dynamic record key with `KafkaConstants.KEY`:

```java
.setHeader(KafkaConstants.KEY).simple("${body.endpointId}")
.to("kafka:webhook-events")
```

An endpoint `key` option takes precedence over `KafkaConstants.KEY`.
Do not set an endpoint key when each record needs a different key.

Set `KafkaConstants.PARTITION_KEY` only when the application must select an exact partition.
Prefer a stable record key and the Kafka partitioner for normal production code.

Remove source transport headers before a new publication. Then set only the required producer headers.
This prevents source topic, partition, offset, and manual-commit data from entering the new record.

## Producer acknowledgement

`requestRequiredAcks` controls the broker acknowledgement required for a successful send:

| Value | Meaning |
|---|---|
| `0` | Do not wait for a broker acknowledgement. A send can appear successful before a broker stores it. |
| `1` | Wait for the partition leader to store the record. Do not wait for all in-sync replicas. |
| `all` | Wait for all in-sync replicas. This is the strongest acknowledgement. |

Gregor uses `request-required-acks: all`.

Camel Kafka producer work is callback-based by default.
Camel continues the exchange after Kafka reports success or failure.
A successful producer EIP means Kafka met the configured acknowledgement rule.
The producer option `synchronous=true` forces synchronous route processing. It is not an acknowledgement level.

`deliveryTimeoutMs` bounds the total time for send delay, broker acknowledgement, and producer retries.
`requestTimeoutMs` bounds one broker request.

Kafka producer idempotence is enabled by default when no conflicting option disables it.
It requires `requestRequiredAcks=all`, retries greater than zero, and at most five in-flight requests per connection.
Producer idempotence does not make an HTTP request and a later Kafka publication one atomic operation.

## Manual offset commits

Use all three settings for Gregor consumers:

```yaml
camel:
    component:
        kafka:
            auto-commit-enable: false
            allow-manual-commit: true
            break-on-first-error: true
```

With `allowManualCommit=true`, Camel puts a `KafkaManualCommit` object in the message header named by
`KafkaConstants.MANUAL_COMMIT`.

```java
KafkaManualCommit manualCommit = exchange.getMessage().getHeader(
        KafkaConstants.MANUAL_COMMIT,
        KafkaManualCommit.class
);
manualCommit.commit();
```

Commit only after all required side effects and Kafka publications complete successfully.
This order gives at-least-once processing. A process stop before the commit causes Kafka to deliver the record again.

Do not commit in a child exchange that has lost the source manual-commit header.
Do not copy the manual-commit header to another Kafka record.

`commitTimeoutMs` limits a synchronous offset commit.

## Consumer failures

With `breakOnFirstError=false`, the consumer continues after a failed exchange.
With `breakOnFirstError=true`, it stops the current processing sequence.
The commit manager and manual-commit behavior then control the next offset.

Do not leave a poison record in an endless failure loop.
Classify invalid records and give them an explicit terminal path.

`pollOnError` handles failures from Kafka polling.
Its values include `DISCARD`, `ERROR_HANDLER`, `RECONNECT`, `RETRY`, and `STOP`.
This option does not replace route error handling for a record that was read successfully.

## Poll interval and long processing

`maxPollIntervalMs` is the maximum delay between consumer `poll()` calls.
Kafka removes and reassigns partitions when processing prevents polling for longer than this value.

Set the value above the longest permitted record-processing time.
Include HTTP timeouts, retry waits, Kafka publication time, and offset commit time.

`maxPollRecords` limits the records returned by one poll.
Gregor uses `1` to keep one in-flight record per consumer poll.
This setting reduces batch work. It does not remove the `maxPollIntervalMs` limit.

## Relevant component options

| Camel option | Purpose | Gregor rule |
|---|---|---|
| `brokers` | Set Kafka bootstrap servers. | Use an environment override. |
| `groupId` | Set the consumer group. | Set it on every consumer URI. |
| `consumersCount` | Set consumers per endpoint. | Bind it to listener concurrency. |
| `autoCommitEnable` | Enable periodic automatic commits. | Set `false`. |
| `allowManualCommit` | Add `KafkaManualCommit` to the exchange. | Set `true`. |
| `breakOnFirstError` | Stop after a failed record. | Use `true` and a terminal path. |
| `autoOffsetReset` | Select the start point without a valid committed offset. | Use `earliest` for local behavior. |
| `maxPollRecords` | Limit records per poll. | Set `1`. |
| `maxPollIntervalMs` | Limit time between polls. | Keep above maximum processing time. |
| `requestRequiredAcks` | Set producer durability acknowledgement. | Set `all`. |
| `keySerializer` | Serialize record keys. | Use `StringSerializer`. |
| `valueSerializer` | Serialize record values. | Use the Apicurio Avro serializer. |
| `keyDeserializer` | Deserialize record keys. | Use `StringDeserializer`. |
| `valueDeserializer` | Deserialize record values. | Use the Apicurio Avro deserializer. |
| `additionalProperties` | Pass extra Kafka or serializer properties. | Use it for Apicurio settings. |

## Delivery boundary

Kafka offsets and producer acknowledgements do not make external HTTP delivery exactly once.
A process stop can occur after HTTP delivery and before attempt publication or offset commit.
The next consumer can then send the HTTP request again.

Keep event IDs stable. Make persistence idempotent where possible. Document each duplicate-delivery window.
