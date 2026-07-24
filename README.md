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
| `KAFKA_RETRY_TOPIC_RETENTION`          | `7d`                                                |
| `KAFKA_DEAD_LETTER_TOPIC_RETENTION`    | `30d`                                               |

`WEBHOOK_MAXIMUM_ATTEMPTS` must be from `1` through `4`. `KAFKA_RETRY_TOPIC_RETENTION` must exceed `1h`, and `KAFKA_DEAD_LETTER_TOPIC_RETENTION` must be positive.

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

Delivery state lives on the durable `WebhookEvent` envelope rather than in worker memory. The logical attempt number of an HTTP request is the envelope's `deliveryAttemptCount` at the time of that request plus `1`, and the envelope carried
onward reports the count of the request just performed. Each published attempt takes its `attempt_number`, `retry_count`, and `replay_count` from that envelope, and `dead_lettered` is always `false` in this phase.

Nothing republishes an updated envelope yet, so a source record redelivered by Kafka arrives with `deliveryAttemptCount=0` and restarts numbering at logical attempt `1`.

Before every HTTP request, including each immediate retry re-entry, the delivery path rejects an envelope whose delivery state is impossible:

- `deliveryAttemptCount < 0`
- `retryCount < 0`
- `replayCount < 0`
- `deliveryAttemptCount >= WEBHOOK_MAXIMUM_ATTEMPTS`

A rejected record is discarded loudly. One ERROR log line records the event, tenant, endpoint, all three counters, and the configured maximum, then the source offset commits so the partition keeps making progress. No HTTP request, attempt
record, or attempt row is produced. A non-null `nextEligibleAt` is not a rejection reason and does not delay or skip delivery: delayed retries remain inactive.

## Inspect attempts

```bash
docker compose exec -T postgres psql -U gregor -d gregor -c \
    'SELECT event_id, attempt_number, outcome, http_status, latency_ms, attempted_at, retry_count, replay_count, dead_lettered, details FROM delivery_attempt ORDER BY attempted_at DESC;'
```

The `details` column is PostgreSQL JSONB. Attempt rows do not contain the webhook payload or signing secret.

Kafka declares this topic topology:

| Topic                        | Retention                                           |
|------------------------------|-----------------------------------------------------|
| `webhook-events`             | 7 days                                              |
| `persist-delivery-attempt`   | 30 days                                             |
| `webhook-events-retry-1m`    | Configured retry retention (`7d` by default)        |
| `webhook-events-retry-10m`   | Configured retry retention (`7d` by default)        |
| `webhook-events-retry-1h`    | Configured retry retention (`7d` by default)        |
| `webhook-events-dead-letter` | Configured dead-letter retention (`30d` by default) |

All topics use 12 default partitions and replication factor 1. The four retry and dead-letter topics have no producers or consumers. Delayed retries and dead-lettering remain inactive, and retryable failures still run immediately up to the
configured maximum attempt count.

## Local receiver and signatures

`POST /api/mocked-webhook-receiver` is a local-development facility. It reads the request body without modification and verifies the `webhook-id`, `webhook-timestamp`, and
`webhook-signature` headers. A valid, current Standard Webhooks `v1` HMAC-SHA256 signature returns `204 No Content`. Missing, malformed, nonmatching, stale, or future signature metadata returns an
empty `401 Unauthorized` response. The default timestamp tolerance is five minutes in either direction, including the boundary.

## Safety boundaries

Both HTTP endpoints are publicly reachable without application authentication or authorization. This phase has no SSRF protection, rate limiting, tenant isolation, or production-safe secret
management. The configured signing secret is a local-development default and must not be used in production. Do not expose this service to untrusted networks or use it as a production webhook
platform.
