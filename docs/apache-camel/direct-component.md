# Apache Camel Direct component

Use the Direct component to connect routes in the same `CamelContext`.
A Direct producer invokes the matching Direct consumer through the same exchange.

Upstream source: https://camel.apache.org/components/next/direct-component.html

## URI

Use this URI form:

```text
direct:name
```

The name identifies the in-memory endpoint. It must match between the producer and consumer.

```java
// @formatter:off
from("kafka:webhook-events?groupId=webhook-events")
    .to("direct:webhook-delivery-attempt");

from("direct:webhook-delivery-attempt")
    .bean(this.webhookService, "sendWebhookEvent");
// @formatter:on
```

## Processing model

Direct processing has these properties:

- It stays in one `CamelContext`.
- It does not use a network transport.
- It does not create a durable queue.
- It passes the same exchange to the consumer route.
- It invokes the consumer synchronously from the producer route.
- It returns control after the consumer route completes.
- It propagates a consumer failure to the producer route through the exchange.

Do not use `direct:` as a durability boundary. A process stop loses work that is not held by the source transport.
Use Kafka when work must survive a process stop.

The SEDA component is the related asynchronous in-memory option. Gregor does not use SEDA for durable delivery work.

## No active consumer

The Direct producer uses these main options:

| Option | Default | Result |
|---|---:|---|
| `block` | `true` | Wait for a consumer when no consumer is active. |
| `timeout` | `30000` | Set the maximum wait in milliseconds when `block` is enabled. |
| `failIfNoConsumers` | `true` | Throw an exception if no consumer is available. |

Example:

```text
direct:webhook-delivery-attempt?block=true&timeout=5000&failIfNoConsumers=true
```

Keep `failIfNoConsumers=true` for required internal routes. A missing route must fail instead of dropping the exchange.
Set a bounded timeout when startup order can leave the consumer inactive.

## Exchange pattern and threads

A Direct consumer supports `InOnly` and `InOut` exchange patterns.
The Direct call still completes the consumer route before it returns to the producer route.

The `synchronous` producer option forces continuation on the same thread after asynchronous work in the called route.
It does not add persistence or transaction support.

## Use in Gregor

Use Direct endpoints for these cases:

- Share one delivery path between Kafka consumer routes.
- Split a large Java DSL tree into named route units.
- Make internal boundaries visible in route tests.

Do not use Direct endpoints to claim asynchronous behavior, delivery acknowledgement, or cross-instance communication.
