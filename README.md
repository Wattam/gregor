# Gregor

Gregor is a local-development Spring Boot webhook delivery service. It uses Apache Camel routes as the messaging backbone to accept JSON events, publish versioned Avro records through Kafka and Apicurio Registry, send signed HTTP webhooks,
and store delivery attempts in PostgreSQL.

## Run locally

Start Kafka, Apicurio Registry, and PostgreSQL:

```bash
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

Each valid Kafka consumption performs at most one HTTP request. The attempt number is the incoming `deliveryAttemptCount + 1`. The attempt audits the incoming `retryCount` and `replayCount`, with `dead_lettered=false`.

HTTP `2xx` succeeds. Connection failures, timeouts, `408`, `429`, and `5xx` are retryable. Redirects and other `4xx` responses are permanent failures.

The delivery result route schedules a successor only after a retryable failure below the attempt limit and with a higher tier available. The maximum defaults to 4 and accepts 1–4. The successor increments `retryCount`, carries the completed `deliveryAttemptCount`, and is keyed by `endpointId`. The envelope count selects the tier regardless of the incoming Kafka topic.

| Successor `retryCount` | Topic | Delay |
|---:|---|---|
| 1 | `webhook-events-retry-1m` | 1 minute |
| 2 | `webhook-events-retry-10m` | 10 minutes |
| 3 | `webhook-events-retry-1h` | 1 hour |

`nextEligibleAt` is the HTTP completion time plus the tier delay, truncated to millisecond precision. Success, permanent failure, attempt exhaustion, and absence of a higher tier produce no successor.

The source consumer routes directly to the shared delivery path. It ignores `nextEligibleAt`. A source record with a future `nextEligibleAt` does not wait.

Before every HTTP request, the shared delivery path rejects these impossible states:

- `deliveryAttemptCount < 0`
- `retryCount < 0`
- `replayCount < 0`
- `deliveryAttemptCount >= WEBHOOK_MAXIMUM_ATTEMPTS`

For a source record, rejection writes one ERROR log line with the event, tenant, endpoint, all three counters, and the configured maximum. The source offset then commits. Rejection produces no HTTP request, attempt record, or attempt row.

Each retry consumer first checks the same impossible states. It also rejects a retry envelope when `nextEligibleAt` is null or later than the current time plus the maximum permitted wait. A topic and `retryCount` mismatch is valid. Rejection writes one sanitized ERROR log line. It does not include the payload or signing secret. The retry consumer then commits and discards the record. It produces no HTTP request or downstream record.

An accepted retry record waits synchronously until `nextEligibleAt`. A due or overdue record proceeds without waiting, including after startup or partition assignment. Pending retries survive worker restart within Kafka retention and retain their stored eligibility and counters. An interrupted wait restores the interrupt flag, fails processing, and leaves the offset uncommitted. Redelivery evaluates eligibility again.

Later source records on the same partition and endpoint can pass a pending retry. Endpoint keys provide no FIFO ordering across topics. A synchronous eligibility wait blocks every partition assigned to that retry consumer, but does not block other consumers or groups.

## Publication order and redelivery

HTTP delivery finishes first. Kafka must acknowledge the attempt before the delivery result route publishes any required retry. Kafka must acknowledge all required publications before the source or retry offset commits. The PostgreSQL consumer persists the acknowledged attempt independently. Neither retry publication nor delivery-offset commit waits for a database row.

A failure before the attempt becomes durable can leave the first physical request without an audit row. A failed attempt publication does not schedule a retry. A failure after Kafka acknowledges the attempt but before the retry becomes durable can repeat HTTP despite the durable attempt record.

If Kafka stores a retry but its acknowledgement is lost, or if Kafka acknowledges the retry but the consumed offset does not commit, redelivery can publish another copy of the same logical successor. Copies have the same event ID and counters but can have different eligibility times. They can produce duplicate delayed HTTP requests and further duplicate successors. A later success does not cancel a published retry.

Repeated requests carry the same `webhook-id`. Redelivery of the same envelope repeats the logical attempt number. PostgreSQL deduplication by `(event_id, attempt_number)` does not prevent HTTP requests or deduplicate Kafka retries. Receivers must handle repeated IDs idempotently.

## Inspect attempts

```bash
docker compose exec -T postgres psql -U gregor -d gregor -c \
    'SELECT event_id, attempt_number, outcome, http_status, latency_ms, attempted_at, retry_count, replay_count, dead_lettered, details FROM delivery_attempt ORDER BY attempted_at DESC;'
```

The `details` column is PostgreSQL JSONB. Attempt rows do not contain the webhook payload or signing secret.

Apicurio registry clients share a Spring-managed Vert.x instance that closes on application shutdown. Gregor adds only its generated `WebhookEvent`, `DeliveryAttempt`, and `DeliveryOutcome` classes to Avro’s default trust policy at startup.

Kafka declares this topic topology:

| Topic | Retention | Producer | Consumer group |
|---|---|---|---|
| `webhook-events` | 7 days | `POST /api/events` | Source delivery (`webhook-events`) |
| `persist-delivery-attempt` | 30 days | Delivery result route | PostgreSQL persistence (`persist-delivery-attempt`) |
| `webhook-events-retry-1m` | Configured retry retention (`7d` by default) | Delivery result route | Time-gated 1-minute tier (`webhook-events-retry-1m`) |
| `webhook-events-retry-10m` | Configured retry retention (`7d` by default) | Delivery result route | Time-gated 10-minute tier (`webhook-events-retry-10m`) |
| `webhook-events-retry-1h` | Configured retry retention (`7d` by default) | Delivery result route | Time-gated 1-hour tier (`webhook-events-retry-1h`) |
| `webhook-events-dead-letter` | Configured dead-letter retention (`30d` by default) | None | None |

All topics use 12 default partitions and replication factor 1.

## Local receiver and signatures

`POST /api/mocked-webhook-receiver` is a local-development facility. It reads the request body without modification and verifies the `webhook-id`, `webhook-timestamp`, and
`webhook-signature` headers. A valid, current Standard Webhooks `v1` HMAC-SHA256 signature returns `204 No Content`. Missing, malformed, nonmatching, stale, or future signature metadata returns an
empty `401 Unauthorized` response. The default timestamp tolerance is five minutes in either direction, including the boundary.

## Safety boundaries

Both HTTP endpoints are publicly reachable without application authentication or authorization. Gregor has no SSRF protection, rate limiting, tenant isolation, or production-safe secret
management. The configured signing secret is a local-development default and must not be used in production. Do not expose this service to untrusted networks or use it as a production webhook
platform.
