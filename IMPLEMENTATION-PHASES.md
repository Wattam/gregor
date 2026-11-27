# Webhook Delivery Service: Build Phases

This document is the complete implementation roadmap for the webhook delivery service. Phase 1 and Phases 2A through 2H are implemented and are kept
whole. Every remaining phase is broken into the smallest dependency-ordered increment that still has a useful, independently testable
boundary and its own completion gate. Phase 6 stays one release unit because splitting it would advertise an ordering guarantee that does
not hold yet. Completing groundwork or an isolated test suite does not mark an increment as implemented.

## Status and execution order

| Work                | Status                             | Depends on                                                          |
|---------------------|------------------------------------|---------------------------------------------------------------------|
| Phase 1             | Implemented with known limitations | —                                                                   |
| Phase 2A            | Implemented                        | Phase 1                                                             |
| Phase 2B            | Implemented                        | Phase 2A                                                            |
| Phase 2C            | Implemented                        | Phase 2B                                                            |
| Phase 2D            | Implemented                        | Phase 2C                                                            |
| Phase 2E            | Implemented                        | Phase 2D                                                            |
| Phase 2F            | Implemented                        | Phase 2E                                                            |
| Phase 2G            | Implemented                        | Phase 2F                                                            |
| Phase 2H            | Implemented                        | Phase 2G                                                            |
| Phase 2I through 2J | Planned                            | Phase 2H, in listed order                                           |
| Phase 3A through 3J | Planned                            | Phase 2; subphases in listed order                                  |
| Phase 4A through 4F | Planned                            | Stable Phase 2 event contract; can proceed in parallel with Phase 3 |
| Phase 5A through 5R | Planned                            | Phases 2 through 4; subphases in listed order                       |
| Phase 6             | Planned as one release unit        | Phases 2, 3, and 5                                                  |

## Label mapping from the previous roadmap

## Completion rules for every planned increment

- Keep the application runnable and its behavior coherent after the increment. Ship infrastructure ahead of behavior only when the
  increment states exactly which paths stay inactive and proves the active paths unchanged.
- Preserve at-least-once Kafka-to-HTTP behavior and document every new duplicate-delivery window.
- Add unit tests for domain decisions and integration tests for Kafka, PostgreSQL, HTTP, restart, and failure boundaries affected by the
  increment.
- Require Kafka acknowledgement before reporting publication success or committing a source offset.
- Add backward-compatible Avro defaults and regenerate Java from `.avsc` files; never edit generated Avro Java.
- Add database migrations and rollback/compatibility notes before relying on new persisted state.
- Validate new configuration at startup and document its environment overrides.
- Update operational documentation and security boundaries together with the behavior they describe.
- Run the repository's complete test workflow before calling the increment complete.

## Phase 1: Core Webhook Delivery Loop (IMPLEMENTED WITH KNOWN LIMITATIONS)

Phase 1 is complete and is intentionally not divided into smaller tasks. It provides the minimum end-to-end path from event ingestion to
signed webhook delivery and durable logical-attempt history.

### Tasks

- [x] Expose `POST /api/events` as a public REST API that validates events and publishes Avro envelopes to the `webhook-events` Kafka topic.
- [x] Key source records by endpoint ID so Kafka preserves per-endpoint ordering within the source topic.
- [x] Consume events in a delivery-worker consumer group and POST opaque JSON payloads to one configured endpoint.
- [x] Sign requests with Standard Webhooks HMAC-SHA256 headers and a configured `whsec_` secret.
- [x] Make one initial delivery attempt plus at most three immediate retries for connection failures, timeouts, HTTP `408`, HTTP `429`, and
  `5xx` responses.
- [x] Treat `2xx` as success and redirects and other `4xx` responses as permanent failures.
- [x] Complete all immediate attempts for one event before processing the next event from the same Kafka partition.
- [x] Publish every logical delivery attempt to `persist-delivery-attempt` and persist it to PostgreSQL through a separate consumer group.
- [x] Store typed attempt fields with JSONB error or response details while excluding payloads and signing secrets.
- [x] Deduplicate attempt persistence by event ID and attempt number without claiming exactly-once HTTP delivery.
- [x] Expose `POST /api/mocked-webhook-receiver` as a public local receiver that verifies signatures and acts as the default development
  destination.
- [x] Require Kafka acknowledgement before accepting an ingest request or completing an attempt publication.
- [x] Preserve at-least-once delivery when a worker stops after an HTTP request but before committing its Kafka offset.
- [x] Use the local Kafka, PostgreSQL, and Apicurio Registry services with environment-overridable operational configuration where supported.

### Known limitations

- Immediate attempt numbers run from 1 through 4 for each consumption of a source event. If Kafka redelivers the source record, numbering
  starts again at 1.
- PostgreSQL deduplicates `(event_id, attempt_number)`. A repeated physical HTTP request caused by source-record redelivery can therefore
  share a logical attempt number and does not create another database row.
- HTTP delivery happens before attempt publication and source-offset commit. A failure between those operations can leave the first
  physical request without an attempt record before Kafka redelivers the source event. This is expected at-least-once behavior, not
  complete physical-request auditing.
- The attempt limit and response-detail size limit are fixed implementation constants.
- Topic partition and replication settings are externally configurable, while the current 7-day event retention and 30-day attempt
  retention are fixed in code.
- Retries are immediate and have no durable scheduling state.
- There is no retry topic, dead-letter queue, replay, endpoint management, production security, tenant isolation, or strict FIFO mode.
- Endpoint configuration and tenant identity are single configured values rather than managed or authenticated runtime state.

## Phase 2: Retry Topology, Dead-Letter Queue, and Replay (PHASES 2A THROUGH 2H IMPLEMENTED, REST PLANNED)

Phase 2 is split because contracts, retry consumption, retry publication, terminal retention, dead-letter persistence, and replay each have
a separate testable boundary. The subphases must be implemented in order. Retry attempts are sequential within a single non-redelivered
chain, not across duplicate copies of the same event. Later events for the same endpoint may pass an event waiting for delayed retry.
Strict FIFO is deferred to Phase 6.

### Phase 2A: Delivery-state contracts and Kafka topology (IMPLEMENTED)

**Goal:** Add the compatible contracts and infrastructure required by delayed retries without changing Phase 1 delivery behavior yet.

#### Tasks

- [x] Extend `WebhookEvent` with nullable timestamp-millis `nextEligibleAt` defaulting to `null`, integer `deliveryAttemptCount` defaulting
  to `0`, and integer `replayCount` defaulting to `0`.
- [x] Extend `DeliveryAttempt` with integer `retryCount` defaulting to `0`, integer `replayCount` defaulting to `0`, and boolean
  `deadLettered` defaulting to `false`. Keep the existing HTTP outcome values unchanged.
- [x] Regenerate Avro Java and verify old/defaulted records still deserialize. Do not add an explicit schema-version field or a second
  event schema.
- [x] Persist the new attempt audit fields without storing customer payloads or signing secrets.
- [x] Add `gregor.webhook.maximum-attempts`, default it to `4`, accept values from `1` through `4`, and reject other values at startup.
- [x] Add retry retention through `gregor.kafka.topics.retry-retention` and `KAFKA_RETRY_TOPIC_RETENTION`, default it to 7 days, and require
  it to be greater than 1 hour.
- [x] Add dead-letter retention through `gregor.kafka.topics.dead-letter-retention` and `KAFKA_DEAD_LETTER_TOPIC_RETENTION`, default it to
  30 days, and require it to be positive.
- [x] Declare `webhook-events-retry-1m`, `webhook-events-retry-10m`, `webhook-events-retry-1h`, and
  `webhook-events-dead-letter` with delete cleanup, the configured default partition count and replication factor, and the appropriate
  configured retention.
- [x] Use the established hand-built Camel topology and existing partition/replication configuration; do not introduce a second listener
  model through Spring Kafka `@RetryableTopic`.
- [x] Add contract, schema-compatibility, topic-configuration, persistence-mapping, and startup-validation tests.

#### Completion gate

The service still performs the Phase 1 delivery loop, but all compatible contracts, persistence columns, validated settings, and Kafka
topics needed by the next increment exist and pass the full test suite.

#### Status notes

- The attempt limit and both new retention values are now validated configuration. This supersedes the fixed-constant attempt-limit
  entry in the Phase 1 known limitations. The response-detail size limit remains a fixed constant.
- The three retry topics and the dead-letter topic exist with no producers and no consumers.
- Retryable failures still run immediately inside one consumption, so every Phase 1 duplicate-delivery window is unchanged.

### Phase 2B: Envelope-driven delivery state and attempt numbering (IMPLEMENTED)

**Goal:** Make delivery state and logical attempt numbering come from the durable event envelope while the immediate loop still performs
delivery.

#### Tasks

- [x] Initialize every delivery-state field on ingestion and treat the envelope as the only source of delivery state.
- [x] Derive the logical attempt number of each HTTP request from `deliveryAttemptCount` instead of an in-memory route counter.
- [x] Advance `deliveryAttemptCount` on the envelope carried through the delivery route so numbering stays cumulative for later tiers.
- [x] Capture the completion time of each HTTP request so a later increment can measure delay eligibility from the preceding failure.
- [x] Reject impossible envelope state, such as negative counters or a delivery count already at the configured maximum, without issuing an
  HTTP request, and define its operational handling.
- [x] Keep outcome classification exactly as Phase 1 defines it: `2xx` succeeds; connection failures, timeouts, `408`, `429`, and `5xx`
  retry; redirects and other `4xx` responses are terminal.
- [x] Publish attempts with the envelope-derived attempt number and the envelope's `retryCount` and `replayCount` values.
- [x] Test cumulative numbering, rejected envelope state, unchanged classification, unchanged attempt deduplication, and unchanged
  offset-commit ordering.

#### Completion gate

Attempt numbering and delay inputs come from the event envelope, while externally observable delivery behavior is still the Phase 1
immediate loop.

#### Status notes

- An envelope with impossible delivery state is discarded loudly: one ERROR log line carrying the event, tenant, endpoint, all three counters, and the configured maximum, followed by a source-offset commit. Rejection never throws, because
  `break-on-first-error: true` with manual commit and `disableRedelivery()` would otherwise redeliver the record forever and stall the partition. Later Phase 2 increments replace this discard with dead-letter publication.
- `gregor.webhook.maximum-attempts` now bounds the cumulative `deliveryAttemptCount` carried by the envelope instead of a per-consumption counter.
- Nothing republishes an updated envelope yet, so a redelivered source record still arrives with `deliveryAttemptCount=0` and restarts at logical attempt `1`. The Phase 1 known limitation about restarted numbering is unchanged.
- The captured HTTP completion instant is in-memory state for one consumption only. It is not persisted, published, or logged.
- The recursive immediate retry path remains until Phase 2D, so one consumption still performs up to `gregor.webhook.maximum-attempts` HTTP requests and every Phase 1 duplicate-delivery window is unchanged.
- At Phase 2B completion, the three retry topics and the dead-letter topic had no producers and no consumers.

### Phase 2C: Retry-tier consumers and eligibility gate (IMPLEMENTED)

**Goal:** Make the three retry topics consumable and time-gated before anything publishes to them.

#### Tasks

- [x] Add one consumer route per tier with its own consumer group, using the established hand-built Camel topology rather than Spring Kafka
  `@RetryableTopic`.
- [x] Enforce `nextEligibleAt` before an HTTP request, and document the waiting mechanism together with its effect on consumer liveness and
  poll-interval limits.
- [x] Run overdue records immediately after startup, partition assignment, or recovery.
- [x] Reject malformed retry envelopes without issuing an HTTP request, and define their operational handling explicitly.
- [x] Route eligible retry records through the shared delivery path so classification, attempt publication, and numbering match the source
  path.
- [x] Require attempt-publication acknowledgement before committing a retry-topic offset.
- [x] Test the eligibility boundary before, exactly at, and after the eligible instant, plus overdue records at startup, malformed records,
  per-tier consumer groups, and offset-commit ordering.

#### Completion gate

Every retry topic has a working, time-gated consumer and per-tier consumer group, while no producer publishes to it yet and automatic
delivery behavior is unchanged.

#### Status notes

- `webhook-events-retry-1m`, `webhook-events-retry-10m`, and `webhook-events-retry-1h` each have an active consumer group with the same name as its topic.
- No service route produces a retry or dead-letter record. The dead-letter topic has no consumer.
- `camel.component.kafka.max-poll-interval-ms` defaults to 75 minutes. The maximum permitted eligibility wait is the configured interval minus five minutes and defaults to 70 minutes.
- Eligibility waiting is synchronous. A wait blocks all partitions assigned to that consumer. Other consumers in the same group and all other consumer groups continue independently.
- A malformed retry envelope has impossible delivery state, a null `nextEligibleAt`, or a `nextEligibleAt` after the maximum permitted wait. The route writes one sanitized ERROR log line, discards the envelope without HTTP or downstream publication, and commits the retry offset. A topic and `retryCount` mismatch remains valid.
- An interrupted wait restores the thread interruption state and fails the exchange. The retry offset stays uncommitted, and Kafka redelivery evaluates eligibility against the current time.
- The source route remains connected directly to the delivery route. It does not evaluate or wait for `nextEligibleAt`.
- Recursive immediate retries remain active through the configured cumulative maximum.
- Retry delivery completes HTTP before attempt-publication acknowledgement and completes all attempt publications before retry-offset commit. A failure in this window can repeat the physical request with the same `webhook-id` and logical attempt number.

### Phase 2D: Kafka-backed delayed retry cutover (IMPLEMENTED)

**Goal:** Replace recursive immediate retries with durable, endpoint-keyed Kafka retry tiers.

#### Tasks

- [x] After a retryable failure, publish an updated event to the next retry topic with the same event ID, tenant ID, endpoint ID, payload,
  and endpoint Kafka key.
- [x] Derive `nextEligibleAt` from the captured completion time of the failed request and the fixed 1-minute, 10-minute, and 1-hour tier
  delays.
- [x] Advance `retryCount`, select the tier from it, and honor `gregor.webhook.maximum-attempts` so lower values use a shorter tier subset.
- [x] Remove the recursive immediate retry path so one consumption performs at most one HTTP request.
- [x] Ensure retry-publication and attempt-publication acknowledgement precede the consumed offset commit.
- [x] Verify that a delayed event does not block a later event for the same endpoint and document this non-FIFO behavior.
- [x] Document every duplicate-delivery window created by publishing a retry record after the HTTP request.
- [x] Test all tiers, every valid maximum-attempt setting, restart recovery, source redelivery, retry-publication failure, endpoint keys,
  and offset-commit ordering.

#### Completion gate

Automatic delivery uses one immediate attempt followed by the configured subset of the fixed 1-minute, 10-minute, and 1-hour tiers. An
exhausted event stops after its final tier and produces no dead-letter record yet.

### Phase 2E: Dead-letter publication for terminal events (IMPLEMENTED)

**Goal:** Retain every terminal delivery generation durably in Kafka.

#### Tasks

- [x] Publish exhausted retryable events to the dead-letter topic with the same identity, payload, cumulative counters, and endpoint Kafka
  key.
- [x] Publish permanently failed events to the dead-letter topic without a further retry publication.
- [x] Mark the terminal attempt record as dead-lettered and leave every earlier attempt record unchanged.
- [x] Keep payloads opaque to delivery workers. Use only Gregor-generated failure details in attempt records.
- [x] Ensure dead-letter and attempt publication are acknowledged before the consumed offset commits.
- [x] Define behavior when dead-letter publication fails, and never drop a terminal event silently.
- [x] Test exhaustion from each tier, permanent failure on the first attempt, publication failure and redelivery, duplicate dead-letter
  records, and terminal attempt flags.

#### Completion gate

A committed terminal result has an acknowledged dead letter and terminal attempt. There is no Gregor dead-letter consumer.

### Phase 2F: Dead-letter occurrence model and envelope codec (IMPLEMENTED)

**Goal:** Define a persistable, replay-ready representation of one terminal delivery generation.

#### Tasks

- [x] Define a dead-letter occurrence model keyed by a unique occurrence ID and unique event/replay generation.
- [x] Store the original opaque Avro event envelope, dead-letter timestamp, replay state, and replay generation; never store a signing
  secret.
- [x] Add a binary Avro codec for storing and reconstructing the exact event envelope, including registry-resolved schema handling.
- [x] Add retained-occurrence lookup operations for later reads and replay.
- [x] Document the schema addition, rollback expectations, and local database recreation requirement.
- [x] Test envelope round trips across schema versions, generation uniqueness, payload and secret exclusion, and lookup behavior.

#### Completion gate

The envelope codec and occurrence table are available internally. Gregor has no dead-letter consumer or automatic occurrence writer.

### Phase 2G: Idempotent dead-letter persistence (IMPLEMENTED)

**Goal:** Persist each terminal generation exactly once per generation before committing offsets.

#### Tasks

- [x] Consume the dead-letter topic in a separate consumer group.
- [x] Insert occurrences idempotently before committing offsets so a duplicate record cannot create a second occurrence for one generation.
- [x] Use Kafka publication time as the dead-letter time so redelivery does not extend retention.
- [x] Fail the consumption without committing when persistence fails so Kafka redelivers the record.
- [x] Test duplicate consumption, persistence failure and redelivery, restart recovery, and repeated generations.

#### Completion gate

Every terminal event is an idempotently persisted dead-letter occurrence that is written before its offset commits.

### Phase 2H: Dead-letter retention and cleanup (IMPLEMENTED)

**Goal:** Bound retained dead-letter state without hiding it from operators before it expires.

#### Tasks

- [x] Exclude expired occurrences from every read immediately at the configured dead-letter retention boundary.
- [x] Add scheduled physical cleanup with a validated interval and bounded batch size.
- [x] Keep cleanup safe with concurrent readers, replay requests, and multiple instances.
- [x] Add retained-volume and cleanup metrics and logs without payloads or signing secrets.
- [x] Test the expiry boundary just inside and just outside retention, cleanup batching, concurrent cleanup, and read exclusion before
  physical deletion.

#### Completion gate

Retained occurrences are retention-bounded, expired state disappears from reads immediately, and physical cleanup is observable.

### Phase 2I: Single-event replay API

**Goal:** Replay one retained dead-letter occurrence at most once without changing event identity.

#### Tasks

- [ ] Expose `POST /api/dead-letter-events/{eventId}/replay` with an empty request body.
- [ ] Add an atomic replay operation that locks the latest retained occurrence and accepts it at most once.
- [ ] Preserve the original event ID, type, tenant, endpoint, creation time, payload bytes, cumulative attempt count, and endpoint Kafka
  key.
- [ ] Start a fresh retry cycle by incrementing replay generation and resetting retry tier and next-eligible time.
- [ ] Publish replay to the source topic synchronously and mark the occurrence accepted only after Kafka acknowledges it.
- [ ] Return `202 Accepted` with `{"eventId":"<original-event-id>"}` after acknowledgement, `404` for unknown or expired state, `409`
  for an already accepted occurrence, and `503` for publication failure while leaving the occurrence eligible for another request.
- [ ] Document that the unauthenticated Phase 2 replay endpoint is for trusted local use only. Authentication is added in Phase 5A.
- [ ] Test concurrent replay requests for one occurrence, failed publication, unknown and expired state, retention races, and acceptance
  ordering relative to Kafka acknowledgement.

#### Completion gate

A retained occurrence is replayed at most once and re-enters the normal delivery path with its original identity and cumulative attempt
count.

### Phase 2J: Replay-aware delivery auditing

**Goal:** Make repeated delivery cycles for one event independently auditable.

#### Tasks

- [ ] Propagate replay generation through delivery, retry tiers, attempt records, and dead-letter records so each generation is
  attributable.
- [ ] Keep attempt numbering cumulative across generations and keep attempt deduplication correct when a later generation continues the
  sequence.
- [ ] Create a new dead-letter occurrence for each exhausted generation without mutating an earlier occurrence.
- [ ] Verify that `webhook-id` stays stable while every physical request receives a fresh webhook timestamp and signature.
- [ ] Record replayed attempts in the normal attempt stream; do not create a synthetic attempt merely for replay acceptance.
- [ ] Document replay semantics, cumulative numbering, and the interaction between replay and dead-letter retention.
- [ ] Test repeated generations, retry exhaustion after replay, duplicate-safe persistence across generations, and stable identity with
  fresh signatures.

#### Completion gate

A retained event can complete repeated, independently auditable delivery cycles while only one replay is accepted per dead-letter
occurrence and all Phase 2 acceptance criteria pass.

## Phase 3: Endpoint Health and Delivery Protection (PLANNED)

Phase 3 is split so that every protection mechanism gets a durable state model before it is allowed to change delivery decisions. This order
avoids implementing circuit breakers, disablement, or rate limits before there is trustworthy endpoint state to support them.

### Phase 3A: Endpoint health state model and projection

**Goal:** Build a reliable endpoint-health projection from delivery results before allowing it to affect routing.

#### Tasks

- [ ] Define endpoint health states, the result signals that affect them, rolling windows, consecutive-failure counters, and recovery rules.
- [ ] Decide and document the source of truth: PostgreSQL only or PostgreSQL plus a compacted Kafka state topic.
- [ ] Add a durable endpoint operational-state model that is separate from immutable attempt history.
- [ ] Update health state idempotently from attempt events, including duplicates, retries, replay generations, and out-of-order persistence.
- [ ] Define the meaning and bounds of recent-failure information, and keep it free of payloads and signing secrets.
- [ ] Keep the projection non-authoritative: no delivery decision reads it in this increment.
- [ ] Test state transitions, duplicate attempt events, concurrent updates, restart recovery, stale events, and tenant/endpoint key
  correctness.

#### Completion gate

A durable, idempotent health projection exists and is verified against the attempt stream, while no routing decision and no API reads it.

### Phase 3B: Endpoint health API and observability

**Goal:** Let operators inspect endpoint health without changing delivery behavior.

#### Tasks

- [ ] Expose a read-only endpoint-health API with current state, transition time, and sanitized recent-failure information.
- [ ] Define pagination, filtering, and response stability for many endpoints.
- [ ] Add health transition metrics and logs with bounded cardinality and without endpoint secrets or payloads.
- [ ] Document the trusted-local exposure boundary until Phase 5A adds authentication.
- [ ] Test API responses against the projection, redaction, unknown endpoints, pagination stability, and metric emission.

#### Completion gate

Operators can inspect trustworthy endpoint health, but health state does not yet suppress or delay delivery.

### Phase 3C: Circuit-breaker state machine and coordination

**Goal:** Make breaker state correct and shared before it can block a request.

#### Tasks

- [ ] Define configurable closed, open, and half-open thresholds, windows, cooldowns, and probe limits, and validate them at startup.
- [ ] Derive breaker transitions from health signals with an explicit clock and documented time source.
- [ ] Make breaker ownership and transitions safe across multiple worker instances, restarts, and Kafka rebalances.
- [ ] Persist breaker state durably and document its relationship to the health projection.
- [ ] Keep the breaker advisory: no delivery decision consults it in this increment.
- [ ] Test threshold boundaries, concurrent writers, cooldown expiry, restart recovery, and clock movement.

#### Completion gate

Breaker state is durable, coordinated across workers, and verified by tests, while delivery behavior is unchanged.

### Phase 3D: Circuit-breaker enforcement in delivery

**Goal:** Stop repeatedly calling a known-unhealthy endpoint while continuing to process unrelated endpoints.

#### Tasks

- [ ] Check breaker state before HTTP delivery and route an open-breaker event into the delayed path without making a request.
- [ ] Decide whether a breaker deferral consumes retry budget and how it is audited; never represent a skipped request as a physical HTTP
  attempt.
- [ ] Define deferral behavior for initial delivery, each retry tier, and replayed generations.
- [ ] Ensure an unhealthy endpoint cannot block unrelated endpoints that share a Kafka partition.
- [ ] Test open-breaker bypass, retry-budget semantics, partition sharing, offset-commit ordering, and dead-letter interaction.

#### Completion gate

Repeated failures open an endpoint-specific breaker, delivery is deferred according to documented semantics, and healthy endpoints continue
without interruption.

### Phase 3E: Half-open probing and breaker recovery

**Goal:** Recover a breaker automatically without stampeding a recovering endpoint.

#### Tasks

- [ ] Permit bounded half-open probes per cooldown window across concurrent workers.
- [ ] Close the breaker on probe success and reopen it with the documented cooldown on probe failure.
- [ ] Ensure probe selection neither starves an endpoint nor duplicates probe work.
- [ ] Record probe attempts in the normal attempt stream.
- [ ] Test probe limits under concurrency, failed probes, flapping endpoints, recovery, and restart during a probe window.

#### Completion gate

Breakers close or reopen from bounded probe results, and a recovering endpoint receives no more concurrent probes than configured.

### Phase 3F: Endpoint lifecycle model and operator API

**Goal:** Let operators disable and recover endpoints deliberately with durable, auditable state.

#### Tasks

- [ ] Define enabled, manually disabled, automatically disabled, and recovery states with reason, actor, and transition timestamps.
- [ ] Add inspect, disable, and re-enable operations with optimistic concurrency or another explicit lost-update safeguard.
- [ ] Publish auditable lifecycle transitions and expose sanitized recent-failure context.
- [ ] Keep write operations restricted to trusted operational use until Phase 5 adds authentication and authorization.
- [ ] Test repeated commands, concurrent conflicting commands, restart persistence, and audit completeness.

#### Completion gate

Endpoint lifecycle state is durable, operator-controllable, and auditable, while the delivery path does not consult it yet.

### Phase 3G: Disabled-endpoint delivery semantics

**Goal:** Apply disablement consistently to initial delivery, delayed retries, and replay.

#### Tasks

- [ ] Define what happens to initial events, delayed retries, and replays received while an endpoint is disabled.
- [ ] Enforce that decision before HTTP delivery so a disabled endpoint receives no request.
- [ ] Define and implement whether re-enabling releases waiting work automatically or requires operator replay.
- [ ] Keep retry-budget, dead-letter, and audit semantics explicit for disabled-endpoint deferrals.
- [ ] Test disablement during in-flight retries, re-enable release behavior, replay into a disabled endpoint, and restart mid-deferral.

#### Completion gate

Disablement is consistently applied to initial delivery, retries, and replay, with a defined and tested release path after re-enabling.

### Phase 3H: Automatic disablement after sustained failure

**Goal:** Disable an endpoint that keeps failing beyond the configured tolerance.

#### Tasks

- [ ] Add configurable sustained-failure thresholds or periods that trigger automatic disablement, validated at startup.
- [ ] Distinguish automatic from manual disablement in state, audit history, and API responses.
- [ ] Define the interaction with circuit breakers so the two mechanisms neither double-count failures nor contend for state.
- [ ] Define automatic recovery, or require explicit operator action instead.
- [ ] Test manual and automatic races, repeated automatic triggers, breaker interaction, and recovery.

#### Completion gate

Sustained failure durably disables an endpoint with auditable cause, a documented breaker relationship, and an explicit recovery path.

### Phase 3I: Per-endpoint rate limiting

**Goal:** Keep one endpoint from exceeding its delivery policy.

#### Tasks

- [ ] Select and document a per-endpoint rate-limit algorithm, burst behavior, distributed state model, and clock semantics, and validate
  the configuration at startup.
- [ ] Define whether rate-limited events wait, enter a retry tier, or use a separate eligibility mechanism, including audit and retry-budget
  effects.
- [ ] Preserve the existing ordering contract and clearly document that rate limiting does not provide Phase 6 strict FIFO.
- [ ] Add metrics and operator-visible reasons for rate-limited deferral.
- [ ] Test exact rate boundaries, bursts, concurrent workers, clock changes, restart behavior, and retry interaction.

#### Completion gate

Per-endpoint delivery rates are enforced across workers with documented deferral, audit, and ordering tradeoffs.

### Phase 3J: Slow-endpoint isolation and capacity bounds

**Goal:** Prevent one endpoint from monopolizing worker capacity.

#### Tasks

- [ ] Bound connection time, response time, in-flight requests, and worker resources per endpoint.
- [ ] Isolate slow endpoints so unrelated endpoints on the same Kafka partition remain eligible for delivery.
- [ ] Define and implement behavior when per-endpoint capacity is exhausted, including saturation signals and deferral.
- [ ] Document the worker capacity model and its interaction with the configured listener concurrency.
- [ ] Test slow responses, saturated endpoints, partition sharing, restart under saturation, and interaction with breakers and rate limits.

#### Completion gate

Endpoint-specific limits and slow responses cannot exhaust shared delivery capacity, and all deferral and ordering tradeoffs are explicit.

## Phase 4: Spring Boot Starter and Transactional Outbox (PLANNED)

Phase 4 is split into module boundaries, direct publication, outbox storage, relay, concurrency hardening, and operations. It can proceed in
parallel with Phase 3 once the Phase 2 event envelope is stable.

### Phase 4A: Module boundaries and shared event contract

**Goal:** Make the event contract consumable by other applications without duplicating it.

#### Tasks

- [ ] Establish the Gradle module boundaries for the reusable event contract, the starter and auto-configuration, and the service
  application.
- [ ] Package or generate the same Avro contract used by the service without maintaining a divergent envelope definition.
- [ ] Define artifact coordinates, versioning, and the schema-compatibility policy for external consumers.
- [ ] Keep the service build, tests, and runtime behavior unchanged by the module split.
- [ ] Test that the service publishes and consumes identical records after the split and that the contract module carries no service-only
  dependencies.

#### Completion gate

The event contract lives in a separately consumable module, service behavior is unchanged, and no starter is published yet.

### Phase 4B: Starter auto-configuration and direct Kafka publisher

**Goal:** Give Spring Boot applications a conventional, supported way to publish compatible events directly to Kafka.

#### Tasks

- [ ] Define the starter's public producer API, configuration properties, validation, and compatibility policy.
- [ ] Auto-configure the Kafka producer, Apicurio serializer, endpoint-keyed publication, and acknowledgement handling using standard Spring
  Boot conventions.
- [ ] Expose a producer API that creates valid initial envelopes and either returns acknowledgement or reports publication failure without
  false acceptance.
- [ ] Allow applications with custom infrastructure to replace the provided publisher through documented extension points.
- [ ] Add starter slice tests and a sample application proving auto-configuration, override behavior, serialization compatibility, and
  acknowledgement semantics.
- [ ] Document that direct publication is not atomic with an application's database transaction.

#### Completion gate

A separate Spring Boot application can add the starter, configure Kafka and registry access, and publish a service-compatible event without
calling the ingest HTTP API.

### Phase 4C: Outbox storage and transaction participation

**Goal:** Record a webhook intent atomically with the application's domain change.

#### Tasks

- [ ] Define a storage-neutral outbox abstraction and transaction-participating API for recording webhook intents.
- [ ] Choose and implement the first JDBC or JPA backend; leave explicit interfaces for future non-JDBC stores.
- [ ] Decide table ownership and provide versioned migrations, indexes, payload format, status fields, timestamps, and cleanup metadata.
- [ ] Ensure the outbox insert joins the caller's existing database transaction rather than opening an independent transaction.
- [ ] Assign the stable event ID when the intent is recorded so a later relay retry republishes the same identity.
- [ ] Test commit, rollback, propagation behavior, serialization failure, and transaction-manager integration.

#### Completion gate

Committed transactions leave durable, correctly scoped outbox rows and rolled-back transactions leave none, with no relay yet and that
limitation documented.

### Phase 4D: Poll-and-publish outbox relay

**Goal:** Move recorded intents to Kafka without losing or silently duplicating them.

#### Tasks

- [ ] Implement a basic poll-and-publish relay with configurable polling, ordering, batch size, and retry/backoff validated at startup.
- [ ] Mark a row published only after Kafka acknowledgement and preserve enough state to retry an interrupted publication safely.
- [ ] State the duplicate-publication contract clearly: a crash may publish the same stable event ID again, but must not lose a committed
  intent.
- [ ] Test Kafka failure, crash windows at every acknowledgement boundary, restart recovery, and end-to-end arrival on the source topic.

#### Completion gate

A committed application transaction eventually reaches `webhook-events`; a rolled-back transaction never does, and relay failures cause
retry rather than intent loss.

### Phase 4E: Concurrent relay claiming and ordering

**Goal:** Make the relay safe when several application instances run it.

#### Tasks

- [ ] Add row claiming and locking that support concurrent relay instances without long database locks or starvation.
- [ ] Define ordering guarantees across batches, applications, and endpoint keys, and verify they match the service's non-FIFO contract.
- [ ] Add graceful shutdown so claimed work is completed or safely released.
- [ ] Test concurrent pollers, lock expiry, process termination while claims are held, database failover, and Kafka recovery.

#### Completion gate

Multiple application instances relay concurrently with documented ordering, no stuck claims, and no committed-intent loss.

### Phase 4F: Relay operations, cleanup, and starter documentation

**Goal:** Make the outbox operable and supportable in production applications.

#### Tasks

- [ ] Add bounded retries, poison-row handling, and operator recovery for rows that cannot be published.
- [ ] Add cleanup or archival for published rows with configurable retention.
- [ ] Add metrics and logs for backlog age, batch latency, publication failures, duplicate publication, and cleanup.
- [ ] Publish starter reference documentation, schema-compatibility policy, upgrade guidance, and a complete outbox sample.
- [ ] Test poison rows, retry exhaustion, cleanup under load, observability output, and the documented sample application.

#### Completion gate

Relay backlog and failures are observable, poison rows have an operator path, published rows are cleaned up, and upgrade guidance is
published.

## Phase 5: Portal, Multi-Tenancy, and Schema Validation (PLANNED)

Phase 5 is split the most because it combines security, control-plane data, operator APIs, optional schema validation, production
operations, and a user interface. Security comes first so no capability is exposed before isolation exists, and the portal comes last so it
is built on stable, secured APIs rather than becoming a second control plane.

### Phase 5A: Authentication and credential foundation

**Goal:** Establish authenticated identity before any capability depends on it.

#### Tasks

- [ ] Select the authentication provider and protocol, and define user, service credential, role, and permission models.
- [ ] Add tenant and application identities plus scoped credentials with issuance, expiry, revocation, and audit metadata.
- [ ] Add authentication configuration that rejects unauthenticated access to every non-public path.
- [ ] Keep local development usable, including the mocked receiver, and document every temporary allowance explicitly.
- [ ] Test credential issuance, expiry, revocation, malformed and replayed credentials, and clock skew.

#### Completion gate

Every request carries an authenticated principal with resolvable permissions, while capability authorization and tenant isolation follow in
the next increments.

### Phase 5B: Tenant context and database isolation

**Goal:** Derive tenant identity from the principal and enforce isolation in storage.

#### Tasks

- [ ] Derive tenant context from the authenticated principal; stop trusting a configured or caller-supplied tenant as authority.
- [ ] Define the database isolation strategy and record the decision with its tradeoffs.
- [ ] Enforce isolation in keys, constraints, repositories, transactions, and administrative queries.
- [ ] Migrate existing single-tenant configuration and data into the tenant model and document the required local reset.
- [ ] Test repository-level isolation, missing tenant context, and cross-tenant identifier probing.

#### Completion gate

No storage path can read or write another tenant's rows, and tenant identity originates only from the authenticated principal.

### Phase 5C: Capability authorization and tenant propagation

**Goal:** Authorize every existing capability and carry tenant context through the whole pipeline.

#### Tasks

- [ ] Authorize every existing API by capability, including ingest, health, endpoint lifecycle, dead-letter access, and replay.
- [ ] Enforce tenant context through ingestion, event envelopes, delivery attempts, dead letters, health state, replay, logs, metrics, and
  tracing.
- [ ] Give Kafka consumers and scheduled jobs explicit tenant scoping instead of ambient authority.
- [ ] Document trusted administrative access and emergency access without adding an unaudited isolation bypass.
- [ ] Add cross-tenant negative tests at controller, service, repository, Kafka-consumer, and operational-tooling boundaries.

#### Completion gate

No authenticated tenant can read, mutate, replay, or infer another tenant's data, and every privileged operation has an auditable identity.

### Phase 5D: Application and endpoint control plane

**Goal:** Replace static endpoint configuration with tenant-scoped managed configuration.

#### Tasks

- [ ] Define tenant-scoped models and lifecycle rules for applications, endpoints, and delivery policies.
- [ ] Add versioned APIs for create, read, update, disable, and delete operations with pagination and concurrency control.
- [ ] Reconcile Phase 3 operational endpoint state with configured endpoint state so control-plane edits do not erase health history.
- [ ] Resolve endpoint configuration in the delivery path from managed state, keeping a documented local development default.
- [ ] Test authorization, concurrent edits, deletion with events in flight, and preservation of health history.

#### Completion gate

Tenants manage real applications and endpoints, and workers deliver using resolved managed configuration.

### Phase 5E: Event types, subscriptions, and ingest resolution

**Goal:** Route ingested events to subscribed endpoints without exposing endpoint configuration to producers.

#### Tasks

- [ ] Define tenant-scoped models and lifecycle rules for event types and subscriptions.
- [ ] Resolve event type and application subscriptions to endpoint IDs during ingestion, including fan-out to several endpoints.
- [ ] Keep endpoint destinations and signing secrets invisible to producers.
- [ ] Define behavior for unmatched event types, disabled subscriptions, and subscription changes while events are in retry.
- [ ] Test single-endpoint and multi-endpoint fan-out, unmatched events, subscription changes mid-retry, and per-endpoint keying and
  ordering.

#### Completion gate

Producers publish event types and the service resolves subscriptions into per-endpoint deliveries with unchanged ordering and no
configuration leakage.

### Phase 5F: Signing-secret lifecycle

**Goal:** Manage signing secrets safely for the whole delivery path.

#### Tasks

- [ ] Encrypt signing secrets at rest with a documented key-management and key-rotation model.
- [ ] Never return a full existing secret; disclose a secret only once at creation or rotation.
- [ ] Implement signing-secret creation, rotation, overlap/grace behavior, revocation, and auditable operator actions.
- [ ] Keep secrets out of Kafka records, attempt records, dead-letter metadata, API responses, and logs.
- [ ] Test rotation during in-flight retries and replay, revocation, non-disclosure, encryption-key rotation, and audit completeness.

#### Completion gate

Signing secrets are encrypted, rotatable, revocable, never disclosed after creation, and never leave the signing boundary.

### Phase 5G: Destination safety and in-flight configuration changes

**Goal:** Prevent tenant-supplied destinations from reaching protected networks.

#### Tasks

- [ ] Add SSRF protection and destination restrictions covering schemes, ports, redirects, private/link-local addresses, DNS resolution, and
  DNS rebinding.
- [ ] Validate destinations both when configuration is written and immediately before each delivery.
- [ ] Provide an explicit local-development allowance for the mocked receiver and document its production restriction.
- [ ] Define safe behavior for endpoint URL or policy changes while events are in retry, dead-letter, or FIFO queues.
- [ ] Test bypass attempts including redirect chains, rebinding, address-family tricks, and credentials in URLs, plus delivery after a URL
  change.

#### Completion gate

Untrusted destinations cannot be used to reach protected networks, and configuration changes have defined, tested in-flight semantics.

### Phase 5H: Tenant event metadata and history APIs

**Goal:** Let operators inspect event and attempt history through secured APIs.

#### Tasks

- [ ] Add tenant-scoped event metadata storage needed for event inspection without weakening payload-retention rules.
- [ ] Add paginated and filterable APIs for events and delivery attempts with stable sorting and cursor semantics.
- [ ] Bound query cost for large histories and document the required indexes and partitioning.
- [ ] Test tenant isolation, large histories, pagination stability under concurrent writes, and filter correctness.

#### Completion gate

Event and attempt history is inspectable through secured, tenant-scoped APIs with bounded query cost and no direct database access.

### Phase 5I: Payload visibility, dead-letter access, and authorized replay

**Goal:** Expose terminal state and replay under authorization and explicit payload rules.

#### Tasks

- [ ] Define payload visibility, redaction, encryption, and retention separately from payload-free attempt history.
- [ ] Add paginated and filterable APIs for dead-letter occurrences and replay history.
- [ ] Replace the local replay boundary with authorized replay and record actor, reason, time, source occurrence, and result.
- [ ] Preserve the Phase 2I at-most-once acceptance rules under authorization and tenant scoping.
- [ ] Test redaction, expiry, concurrent authorized replay, cross-tenant replay attempts, and recorded replay context.

#### Completion gate

Dead-letter inspection and replay are authorized, tenant-scoped, and fully attributed without weakening at-most-once acceptance.

### Phase 5J: Audit history and API surface normalization

**Goal:** Make privileged change history complete and the API surface consistent enough to build a portal on.

#### Tasks

- [ ] Add immutable audit history for credential, endpoint, subscription, policy, schema, retention, and replay changes.
- [ ] Define audit retention and tamper resistance.
- [ ] Normalize API errors, pagination, sorting, timestamps, and long-running operation behavior for portal consumption.
- [ ] Publish a machine-readable API reference that matches the normalized surface.
- [ ] Test audit completeness across every mutating path, error-shape consistency, and drift between the reference and the implementation.

#### Completion gate

Every privileged change is auditable, and API responses, errors, and pagination behave uniformly across the secured surface.

### Phase 5K: Customer schema registration and compatibility APIs

**Goal:** Let tenants manage event-type schemas in isolation from internal contracts.

#### Tasks

- [ ] Define event-type schema ownership, supported schema formats, versioning, compatibility modes, and registry namespace isolation.
- [ ] Add authorized schema registration, version listing, activation, deprecation, and compatibility-check APIs.
- [ ] Keep customer schemas isolated from the service's internal envelope schemas.
- [ ] Test tenant namespace isolation, compatibility decisions, invalid schemas, and large schema documents.

#### Completion gate

Tenants can register and evolve their own event-type schemas without touching the service's internal contracts or another tenant's
namespace.

### Phase 5L: Optional ingest validation

**Goal:** Optionally validate customer payloads while keeping delivery workers payload-opaque.

#### Tasks

- [ ] Bind an application event type to an active schema and an explicit validation policy.
- [ ] Validate payloads at ingestion before Kafka acceptance and return stable, bounded validation errors.
- [ ] Store only the schema identity and validation result needed for audit; do not make workers parse customer payloads.
- [ ] Define behavior when the registry is unavailable and when an in-flight event references a deprecated schema.
- [ ] Test compatibility changes, invalid payloads, registry failures, large payloads, and validation-disabled event types.

#### Completion gate

Tenants may opt into deterministic schema validation at ingestion without changing the delivery worker's opaque-payload contract.

### Phase 5M: Service-level objectives and tenant-safe observability

**Goal:** Make reliability measurable without leaking tenant data.

#### Tasks

- [ ] Define service-level indicators and objectives for ingest acknowledgement, queue age, delivery latency, success rate, retries,
  dead-letter volume, replay, endpoint health, and outbox backlog.
- [ ] Add tenant-safe metrics, tracing, and structured logs with bounded cardinality.
- [ ] Add dashboards and alerts mapped to the objectives, with documented operator responses.
- [ ] Test cardinality bounds, label safety, trace propagation across Kafka boundaries, and alert-rule correctness.

#### Completion gate

Reliability targets are defined, measured, and alerted on, with bounded cardinality and no tenant data leakage.

### Phase 5N: Retention, storage scaling, and quotas

**Goal:** Bound storage growth and per-tenant consumption.

#### Tasks

- [ ] Add configurable retention policies for event metadata, attempts, dead letters, audit history, and optional payload storage.
- [ ] Implement PostgreSQL partitioning and cleanup where required, and document the measured threshold for moving analytics to a dedicated
  store such as ClickHouse.
- [ ] Add capacity limits and quotas for tenants, endpoints, payload size, replay, schemas, and retained history with explicit rejection
  behavior.
- [ ] Test retention cleanup under load, partition maintenance, quota boundaries, and behavior at storage limits.

#### Completion gate

Storage growth is bounded by tested retention and partitioning, and per-tenant consumption is capped with predictable rejections.

### Phase 5O: Operational readiness and resilience verification

**Goal:** Prove the service can be operated and recovered under production conditions.

#### Tasks

- [ ] Document backup/restore, disaster recovery, secret/key recovery, schema-registry recovery, incident response, and safe upgrades.
- [ ] Verify restore and recovery procedures against a real environment rather than documentation alone.
- [ ] Run load, soak, failure-injection, and retention-cleanup tests with multiple tenants and slow or failing endpoints.
- [ ] Record the supported operating envelope and the capacity evidence behind it.

#### Completion gate

Recovery procedures are tested, failure injection is survivable, and the supported operating envelope is backed by capacity evidence.

### Phase 5P: Portal foundation and session security

**Goal:** Establish a secure portal shell over the secured APIs.

#### Tasks

- [ ] Select the portal technology and deployment boundary without granting it direct PostgreSQL, Kafka, or registry access.
- [ ] Implement sign-in, tenant/application selection, authorization-aware navigation, and safe session handling.
- [ ] Add security headers, CSRF protection, content-security-policy, and secure session and cookie configuration.
- [ ] Establish accessibility and browser-support baselines with automated checks.
- [ ] Test authentication flows, session expiry and revocation, authorization-aware navigation, and header and policy enforcement.

#### Completion gate

Users sign in, select a tenant, and navigate only permitted areas, with no privileged data path outside the secured APIs.

### Phase 5Q: Portal control-plane workflows

**Goal:** Support configuration workflows in the portal without creating a second source of truth.

#### Tasks

- [ ] Add application, endpoint, subscription, event-type, and delivery-policy workflows.
- [ ] Add credential and signing-secret workflows that honor one-time secret disclosure.
- [ ] Add schema registration, activation, deprecation, and compatibility-check workflows.
- [ ] Require explicit confirmation and show consequences for destructive actions, secret rotation, and endpoint disablement.
- [ ] Test validation errors, concurrency conflicts, permission-limited views, and confirmation flows.

#### Completion gate

Every supported control-plane workflow is available in the portal, backed only by the secured APIs.

### Phase 5R: Portal delivery-operations views and release

**Goal:** Complete the portal with delivery-operations visibility and a documented deployment.

#### Tasks

- [ ] Add event, attempt, endpoint-health, dead-letter, replay, and audit views with pagination and redaction.
- [ ] Add portal-initiated replay with explicit confirmation, actor recording, and result feedback.
- [ ] Add end-to-end tenant-isolation tests across every portal view and action.
- [ ] Document portal deployment, configuration, upgrades, and troubleshooting.

#### Completion gate

Tenant operators can perform the supported control-plane and delivery-operations workflows through the portal without weakening API
security or creating a second source of truth.

## Phase 6: FIFO Webhook Delivery (PLANNED AS ONE RELEASE UNIT)

Phase 6 should **not** be split into independently deployable subphases. Strict FIFO is only true when sequencing, delayed retries,
terminal release, restart/rebalance recovery, and replay all agree. Shipping any subset as FIFO would create a false ordering guarantee.
The implementation can still be organized into the following internal task groups, but the feature remains disabled until the entire phase
passes its completion gate.

### Task group 1: Ordering semantics and mode configuration

- [ ] Decide whether FIFO is global, per endpoint, or a separate endpoint type, and define how the mode is changed safely.
- [ ] Define the exact ordering boundary, the sequence assignment point, and behavior for concurrently accepted events.
- [ ] Decide whether batching is supported and how partial or total batch failure affects order.
- [ ] Validate the FIFO configuration at startup and define its interaction with existing delivery configuration.

### Task group 2: Blocking contract and operator commitments

- [ ] Publish the throughput, latency, and head-of-line blocking contract before implementation.
- [ ] Define replay placement relative to newer queued events.
- [ ] Define operator actions for skipping, cancelling, dead-lettering, or releasing a blocked event.
- [ ] Define the observable states an operator can see for a blocked endpoint and the guarantees each state implies.

### Task group 3: Durable sequencing state

- [ ] Design durable per-endpoint sequence and blocked-event state without putting signing secrets into Kafka or queue state.
- [ ] Make sequence assignment and release idempotent under source-record redelivery.
- [ ] Define storage growth, indexing, and retention for sequencing state.

### Task group 4: Worker ownership and fencing

- [ ] Ensure exactly one event per FIFO endpoint is eligible for HTTP delivery at a time while unrelated endpoints remain eligible.
- [ ] Define worker ownership, fencing, and state transitions across concurrent instances and partition rebalances.
- [ ] Prevent a lost or delayed owner from allowing a second in-flight delivery for the same endpoint.

### Task group 5: Sequenced initial delivery

- [ ] Gate initial delivery by endpoint sequence.
- [ ] Preserve stable event identity, cumulative attempt history, and at-least-once HTTP behavior.
- [ ] Keep unrelated endpoints on the same Kafka partition eligible for delivery.

### Task group 6: Retries and terminal release

- [ ] Keep the endpoint blocked while the head event waits in any delayed retry tier.
- [ ] Release the next event only after the head event succeeds or is durably dead-lettered.
- [ ] Keep dead-letter persistence, replay generations, and release decisions consistent with each other.

### Task group 7: Protection-mechanism integration

- [ ] Integrate circuit-breaker deferrals without allowing later FIFO events to overtake the head event.
- [ ] Integrate disabled-endpoint handling and re-enable release without reordering queued events.
- [ ] Integrate rate limiting and slow-endpoint isolation with sequence gating and document the combined latency effect.

### Task group 8: Recovery and replay placement

- [ ] Reconstruct blocked endpoints correctly after restart, crash, consumer-group rebalance, and database or Kafka outage.
- [ ] Apply the chosen replay placement atomically and prevent replay from silently violating order.
- [ ] Verify that recovery never releases a later event before the head event reaches a terminal state.

### Task group 9: Safeguards and operator tooling

- [ ] Add maximum blocked duration, queue depth, age, and storage safeguards with explicit operator-visible failure behavior.
- [ ] Add metrics and APIs for queue position, blocked reason, head-event age, retry eligibility, and manual intervention.
- [ ] Make every manual intervention auditable and impossible to apply silently.

### Task group 10: Verification and rollout

- [ ] Test concurrent ingestion for one endpoint, many independent endpoints, all retry tiers, permanent failures, replay, worker crashes,
  rebalances, circuit breaking, disablement, and rate limiting.
- [ ] Prove that no later event is delivered before the current head succeeds or is durably exhausted, including every recovery boundary.
- [ ] Load-test the expected head-of-line blocking and document capacity differences from standard non-FIFO delivery.
- [ ] Roll out behind an explicit disabled-by-default feature flag and verify migration for existing endpoints before enabling it.

### Completion gate

FIFO may be advertised only after end-to-end tests prove the ordering guarantee across initial delivery, delayed retry, dead-lettering, replay,
worker restart, consumer rebalance, and all Phase 3 protection mechanisms. Unrelated endpoints must continue to make progress.

## Cross-phase rules

- Preserve at-least-once delivery semantics. Never claim exactly-once delivery across Kafka and HTTP.
- Keep `webhook-id` stable across retries and replay so receivers can deduplicate deliveries.
- Preserve ordering within one event's retry sequence. Allow delayed retries to be overtaken unless strict FIFO is explicitly enabled.
- Keep customer payloads opaque to delivery workers except when optional validation is enabled at ingestion.
- Never include signing secrets in Kafka records, attempt records, dead-letter metadata, API responses, or logs.
- Keep attempt publication and persistence separate from the hot HTTP path through Kafka.
- Treat every API as non-production until it has authentication, authorization, tenant isolation, SSRF controls where applicable, and
  documented exposure rules.
- Prefer backward-compatible contract evolution and explicit migrations; do not let a later phase silently reinterpret old event or attempt
  records.
