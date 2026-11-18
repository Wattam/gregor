# Gregor

Gregor is a local-development Spring Boot webhook delivery service. It uses Apache Camel routes as the messaging backbone to accept JSON events, publish versioned Avro records through Kafka and Apicurio Registry, send signed HTTP webhooks,
and store delivery attempts in PostgreSQL.

## Run locally

After this schema change, recreate the local PostgreSQL and Kafka volumes before running Gregor. This reset is required:

```bash
docker compose down --volumes
docker compose up -d
```

Start Gregor:

```bash
bash ./gradlew bootRun
```

The runtime supports these environment overrides:

| Environment variable                   | Default                                             |
|----------------------------------------|-----------------------------------------------------|
| `WEBHOOK_TENANT_ID`                    | `00000000-0000-0000-0000-000000000001`              |
| `WEBHOOK_ENDPOINT_ID`                  | `00000000-0000-0000-0000-000000000002`              |
| `WEBHOOK_RECEIVER_URL`                 | `http://localhost:8080/api/mocked-webhook-receiver` |
| `WEBHOOK_SIGNING_SECRET`               | Local `whsec_` development secret                   |
| `WEBHOOK_CONNECTION_TIMEOUT`           | `2s`                                                |
| `WEBHOOK_RESPONSE_TIMEOUT`             | `5s`                                                |
| `WEBHOOK_RECEIVER_TIMESTAMP_TOLERANCE` | `5m`                                                |
| `WEBHOOK_MAXIMUM_ATTEMPTS`             | `4`                                                 |
| `KAFKA_LISTENER_CONCURRENCY`           | `12`                                                |
| `KAFKA_MAX_POLL_INTERVAL_MS`           | `4500000`                                           |
| `KAFKA_RETRY_TOPIC_RETENTION`          | `7d`                                                |
| `KAFKA_DEAD_LETTER_TOPIC_RETENTION`    | `30d`                                               |

`WEBHOOK_MAXIMUM_ATTEMPTS` must be from `1` through `4`. `KAFKA_MAX_POLL_INTERVAL_MS` must exceed `3900000`. The maximum permitted eligibility wait is this poll interval minus five minutes. The default poll interval is 75 minutes, and the default maximum permitted wait is 70 minutes. `KAFKA_RETRY_TOPIC_RETENTION` must exceed `1h`, and `KAFKA_DEAD_LETTER_TOPIC_RETENTION` must be positive.

## Ingest an event

`POST /api/events` is a local-development facility. It accepts a nonblank `eventType` and any non-null JSON payload.

```bash
curl --include \
    --request POST \
    --header 'Content-Type: application/json' \
    --data '{"eventType":"order.created","payload":{"orderId":"123"}}' \
    http://localhost:8080/api/events
```

Kafka acknowledgement occurs before the API returns a `202 Accepted` response:

```json
{"eventId":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}
```

Each accepted request creates a new event ID. Delivery is at least once: failures before Kafka offset commit can deliver the same event again with the same `webhook-id`.
Receivers must handle duplicate deliveries idempotently. Attempt persistence ignores a duplicate `(event_id, attempt_number)` row.

## Delivery state and attempt numbering

Delivery state lives on the durable `WebhookEvent` envelope rather than in worker memory. The logical attempt number of an HTTP request is the envelope's `deliveryAttemptCount` at the time of that request plus `1`, and the envelope carried onward reports the count of the request just performed. Each published attempt takes its `attempt_number`, `retry_count`, and `replay_count` from that envelope. `dead_lettered` is always `false`.

Nothing republishes an updated envelope yet. A source record that Kafka redelivers arrives with `deliveryAttemptCount=0` and restarts numbering at logical attempt `1`.

The source consumer routes directly to the shared delivery path. It ignores `nextEligibleAt`. A source record with a future `nextEligibleAt` does not wait.

Before every HTTP request, including each immediate retry re-entry, the shared delivery path rejects these impossible states:

- `deliveryAttemptCount < 0`
- `retryCount < 0`
- `replayCount < 0`
- `deliveryAttemptCount >= WEBHOOK_MAXIMUM_ATTEMPTS`

For a source record, rejection writes one ERROR log line with the event, tenant, endpoint, all three counters, and the configured maximum. The source offset then commits. Rejection produces no HTTP request, attempt record, or attempt row.

Each retry consumer first checks the same impossible states. It also rejects a retry envelope when `nextEligibleAt` is null or later than the current time plus the maximum permitted wait. A topic and `retryCount` mismatch is valid. Rejection writes one sanitized ERROR log line. It does not include the payload or signing secret. The retry consumer then commits and discards the record. It produces no HTTP request or downstream record.

An accepted retry record waits synchronously until `nextEligibleAt`. An eligible or overdue record does not wait. The record then enters the shared delivery path. It keeps the envelope attempt numbering and audit counters. Retryable failures still use recursive immediate retries through the configured maximum.

HTTP delivery finishes before attempt publication acknowledgement. All attempt publications finish before the consumed retry offset commits. A failure after an HTTP request and before attempt publication acknowledgement or offset commit can cause Kafka to deliver the record again. This can repeat the physical HTTP request with the same `webhook-id` and logical attempt number. Attempt-publication failure and interrupted eligibility waiting leave the retry offset uncommitted.

## Inspect attempts

```bash
docker compose exec -T postgres psql -U gregor -d gregor -c \
    'SELECT event_id, attempt_number, outcome, http_status, latency_ms, attempted_at, retry_count, replay_count, dead_lettered, details FROM delivery_attempt ORDER BY attempted_at DESC;'
```

The `details` column is PostgreSQL JSONB. Attempt rows do not contain the webhook payload or signing secret.

Apicurio registry clients share a Spring-managed Vert.x instance that closes on application shutdown.

Kafka declares this topic topology:

| Topic                        | Retention                                           | Consumer group                    |
|------------------------------|-----------------------------------------------------|-----------------------------------|
| `webhook-events`             | 7 days                                              | `webhook-events`                  |
| `persist-delivery-attempt`   | 30 days                                             | `persist-delivery-attempt`        |
| `webhook-events-retry-1m`    | Configured retry retention (`7d` by default)        | `webhook-events-retry-1m`         |
| `webhook-events-retry-10m`   | Configured retry retention (`7d` by default)        | `webhook-events-retry-10m`        |
| `webhook-events-retry-1h`    | Configured retry retention (`7d` by default)        | `webhook-events-retry-1h`         |
| `webhook-events-dead-letter` | Configured dead-letter retention (`30d` by default) | None                              |

All topics use 12 default partitions and replication factor 1. Each retry tier has an active consumer group named after its topic. A synchronous eligibility wait blocks every partition assigned to that Kafka consumer. Other consumers in the same retry group can process their assigned partitions. The source consumer group, the attempt-persistence consumer group, and the other retry consumer groups continue independently.

No route produces a retry record or a dead-letter record. The dead-letter topic has no consumer. Automatic delayed retries and dead-lettering remain inactive. An external producer can put a record on a retry topic. Source retryable failures still run immediately up to the configured maximum attempt count.

## Local receiver and signatures

`POST /api/mocked-webhook-receiver` is a local-development facility. It reads the request body without modification and verifies the `webhook-id`, `webhook-timestamp`, and
`webhook-signature` headers. A valid, current Standard Webhooks `v1` HMAC-SHA256 signature returns `204 No Content`. Missing, malformed, nonmatching, stale, or future signature metadata returns an
empty `401 Unauthorized` response. The default timestamp tolerance is five minutes in either direction, including the boundary.

## Safety boundaries

Both HTTP endpoints are publicly reachable without application authentication or authorization. This phase has no SSRF protection, rate limiting, tenant isolation, or production-safe secret
management. The configured signing secret is a local-development default and must not be used in production. Do not expose this service to untrusted networks or use it as a production webhook
platform.
