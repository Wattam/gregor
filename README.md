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

`WEBHOOK_MAXIMUM_ATTEMPTS` must be from `1` through `4`. `KAFKA_MAX_POLL_INTERVAL_MS` must exceed `3900000`. The maximum permitted eligibility wait is this poll interval minus five minutes. The default poll interval is 75 minutes, and the default maximum permitted wait is 70 minutes. Camel's Kafka client idle connection timeout follows the poll interval so the client does not close an idle connection before that interval expires. `KAFKA_RETRY_TOPIC_RETENTION` must exceed `1h`, and `KAFKA_DEAD_LETTER_TOPIC_RETENTION` must be positive.

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

Each valid Kafka consumption performs at most one HTTP request. The attempt number is the incoming `deliveryAttemptCount + 1`. The attempt audits the incoming `retryCount` and `replayCount`. Terminal audits are published after dead-letter acknowledgement with `deadLettered=true`. Other audits have `deadLettered=false`.

HTTP `2xx` succeeds. Connection failures, timeouts, invalid responses, returned `1xx` statuses, `408`, `429`, and `5xx` are retryable. Redirects and other `4xx` responses are permanent failures.

The delivery result route schedules a successor only after a retryable failure below the attempt limit and with a higher tier available. The maximum defaults to 4 and accepts 1–4. The successor increments `retryCount`, carries the completed `deliveryAttemptCount`, and is keyed by `endpointId`. The envelope count selects the tier regardless of the incoming Kafka topic.

| Successor `retryCount` | Topic | Delay |
|---:|---|---|
| 1 | `webhook-events-retry-1m` | 1 minute |
| 2 | `webhook-events-retry-10m` | 10 minutes |
| 3 | `webhook-events-retry-1h` | 1 hour |

For a retry successor, `nextEligibleAt` is the HTTP completion time plus the tier delay, truncated to millisecond precision. Success produces no successor. Permanent failures and retryable failures with no attempt budget or higher tier are terminal and do not schedule another retry. The dead-letter topic receives the completed Avro `WebhookEvent` keyed by `endpointId`. It preserves the original identity, creation time, payload bytes, retry and replay counts, and nullable `nextEligibleAt`. Only `deliveryAttemptCount` increases. The record contains no signing secret, signature, or added failure metadata.

The source consumer routes directly to the shared delivery path. It ignores `nextEligibleAt`. A source record with a future `nextEligibleAt` does not wait.

Before every HTTP request, the shared delivery path rejects these impossible states:

- `deliveryAttemptCount < 0`
- `retryCount < 0`
- `replayCount < 0`
- `deliveryAttemptCount >= WEBHOOK_MAXIMUM_ATTEMPTS`

For a source record, rejection writes one sanitized ERROR log line with the event, tenant, endpoint, all three counters, and the configured maximum. The source offset then commits without an HTTP request or any downstream publication.

Each retry consumer first checks the same impossible states. It also rejects a retry envelope when `nextEligibleAt` is null or later than the current time plus the maximum permitted wait. A topic and `retryCount` mismatch is valid. Rejection writes one sanitized ERROR log line. It does not include the payload or signing secret. The retry consumer then commits and discards the record. It produces no HTTP request or downstream record.

An accepted retry record waits synchronously until `nextEligibleAt`. A due or overdue record proceeds without waiting, including after startup or partition assignment. Pending retries survive worker restart within Kafka retention and retain their stored eligibility and counters. An interrupted wait restores the interrupt flag, fails processing, and leaves the offset uncommitted. Redelivery evaluates eligibility again.

Later source records on the same partition and endpoint can pass a pending retry. Endpoint keys provide no FIFO ordering across topics. A synchronous eligibility wait blocks every partition assigned to that retry consumer, but does not block other consumers or groups.

## Publication order and redelivery

Publication and commit order depend on the HTTP result:

| Result | Order |
|---|---|
| Success | HTTP → attempt acknowledgement → commit |
| Scheduled retry | HTTP → attempt acknowledgement → retry acknowledgement → commit |
| Terminal failure | HTTP → dead-letter acknowledgement → attempt acknowledgement → commit |

The PostgreSQL consumer persists attempts independently. Delivery-offset commit does not wait for a database row.

A failed dead-letter send stops processing before the audit. If processing fails before the dead letter becomes durable, there is no guarantee that the first outcome is recorded. If Kafka stores the dead letter but its acknowledgement is lost, or if processing fails after dead-letter acknowledgement and before commit, redelivery can publish another dead letter for the same `(eventId, replayCount)`.

A failure before the attempt becomes durable can leave the first physical request without an audit row. An attempt acknowledgement lost after storage or a subsequent offset-commit failure can also duplicate audit publications. A failed attempt publication does not schedule a retry. A failure after Kafka acknowledges the attempt but before the retry becomes durable can repeat HTTP despite the durable attempt record.

If Kafka stores a retry but its acknowledgement is lost, or if Kafka acknowledges the retry but the consumed offset does not commit, redelivery can publish another copy of the same logical successor. Copies have the same event ID and counters but can have different eligibility times. They can produce duplicate delayed HTTP requests and further duplicate successors. A later success does not cancel a published retry.

Redelivery, including after a consumer restart, can repeat HTTP with the same `webhook-id` and logical attempt number. The route uses the new HTTP result. A success on redelivery publishes no new dead letter and does not cancel one already stored. PostgreSQL keeps the first inserted audit for each `(event_id, attempt_number)` even when a repeated attempt has a different outcome or flag. It does not prevent HTTP requests or deduplicate Kafka records. Receivers must handle repeated IDs idempotently.

## Inspect attempts

```bash
docker compose exec -T postgres psql -U gregor -d gregor -c \
    'SELECT event_id, attempt_number, outcome, http_status, latency_ms, attempted_at, retry_count, replay_count, dead_lettered, details FROM delivery_attempt ORDER BY attempted_at DESC;'
```

PostgreSQL stores `deadLettered` in the `dead_lettered` column. The `details` column is PostgreSQL JSONB. HTTP failures have `{"type":"HTTP_FAILURE"}` in `details`, with the status and outcome in separate columns. Transport failures record a failure category, exception class, and a fixed message. Gregor sends request payload bytes unchanged without parsing them in the delivery worker. It does not store receiver response bodies in new attempt records.

Apicurio registry clients share a Spring-managed Vert.x instance that closes on application shutdown. Gregor adds only its generated `WebhookEvent`, `DeliveryAttempt`, and `DeliveryOutcome` classes to Avro’s default trust policy at startup.

Kafka declares this topic topology:

| Topic | Retention | Producer | Consumer group |
|---|---|---|---|
| `webhook-events` | 7 days | `POST /api/events` | Source delivery (`webhook-events`) |
| `persist-delivery-attempt` | 30 days | Delivery result route | PostgreSQL persistence (`persist-delivery-attempt`) |
| `webhook-events-retry-1m` | Configured retry retention (`7d` by default) | Delivery result route | Time-gated 1-minute tier (`webhook-events-retry-1m`) |
| `webhook-events-retry-10m` | Configured retry retention (`7d` by default) | Delivery result route | Time-gated 10-minute tier (`webhook-events-retry-10m`) |
| `webhook-events-retry-1h` | Configured retry retention (`7d` by default) | Delivery result route | Time-gated 1-hour tier (`webhook-events-retry-1h`) |
| `webhook-events-dead-letter` | Configured dead-letter retention (`30d` by default) | Delivery result route | None |

All topics use 12 default partitions and replication factor 1.

## Dead-letter occurrence storage

Hibernate `ddl-auto: update` creates the additive PostgreSQL `dead_letter_occurrence` table and its constraints. It does not alter existing attempt rows or Kafka records. Terminal delivery still publishes to `webhook-events-dead-letter` and uses the existing attempt audit path. Gregor has no dead-letter consumer, and the running delivery routes do not insert occurrences.

Internal occurrence storage retains the complete unframed binary Avro writer datum and its resolved writer schema. The schema can reconstruct a typed event without a registry lookup. The table and `webhook-events-dead-letter` both contain customer payloads. Restrict access to trusted local operators. Do not enable external SQL parameter or protocol tracing against these payload stores.

To roll back, run the prior binary with the unused table intact. `DROP TABLE dead_letter_occurrence;` is optional destructive cleanup that deletes all stored occurrences.

To recreate local storage, run the following commands. **This deletes all local PostgreSQL, Kafka, and Apicurio data.**

```bash
docker compose down -v
docker compose up -d
```

## Local receiver and signatures

`POST /api/mocked-webhook-receiver` is a local-development facility. It reads the request body without modification and verifies the `webhook-id`, `webhook-timestamp`, and
`webhook-signature` headers. A valid, current Standard Webhooks `v1` HMAC-SHA256 signature returns `204 No Content`. Missing, malformed, nonmatching, stale, or future signature metadata returns an
empty `401 Unauthorized` response. The default timestamp tolerance is five minutes in either direction, including the boundary.

## Safety boundaries

Both HTTP endpoints are publicly reachable without application authentication or authorization. Gregor has no SSRF protection, rate limiting, tenant isolation, or production-safe secret
management. The configured signing secret is a local-development default and must not be used in production. Do not expose this service to untrusted networks or use it as a production webhook
platform. Restrict access to both dead-letter payload stores to trusted local operators.
