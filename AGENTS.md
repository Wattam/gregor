## Scope

- Read `IMPLEMENTATION-PHASES.md` before implementation. Update the phase status when its increment lands.
- Document only current behavior in `README.md`. Omit roadmap references and future work.

## Java

- Prefer newer finalized JDK features when they simplify the code. Never use preview or incubator features.
- Name fields, parameters, and locals after their declared type (`ObjectMapper objectMapper`). If several variables share a type, prefix their roles (`receivedWebhookEvent`, `attemptedWebhookEvent`). Apply the same rule to constants in uppercase snake case (`TEST_RECEIVER`).
- Use plural names for collections, maps, arrays, and similar containers (`List<WebhookEvent> webhookEvents`). Use descriptive names for primitives, boxed primitives, `String`, `Object`, and value types such as `UUID`, `Instant`, and `Duration`.
- Limit lines to 240 columns. For calls and declarations, put all arguments or parameters on one line or exactly one per line. Never mix. Put `)` on its own line when wrapping inside the parentheses. Break chains before the dot.
- Qualify instance members with `this.` or `super.`. Qualify external statics with their declaring class; leave own statics unqualified. Never use static imports.
- Use Lombok `@Slf4j` with bracketed fields: `log.info("Published event [id={}]", eventId)`.
- Format Camel routes as EIP trees between `// @formatter:off` and `// @formatter:on`. Indent nested steps four spaces, align `.end()` with its opener, and keep each EIP's calls together (`.choice().when().method(...)`).
- Use Jackson 3 (`tools.jackson.databind`), not transitive Jackson 2. Keep core annotations under `com.fasterxml.jackson.annotation`. Inject the configured mapper in production.

## Configuration and tests

- Add metadata for every `camel.*` key in `application.yaml` to `src/main/resources/META-INF/additional-spring-configuration-metadata.json`; Camel does not supply it.
- After changing a persisted column, run `docker compose down -v` before testing. This deletes local PostgreSQL, Kafka, and Apicurio data.
- After Java, test, Avro, Gradle, or application configuration changes, run `docker compose up -d` then `./gradlew test`. Leave the services running.
- Cover status and validation matrices in focused tests. Use full-stack tests for representative wiring and recovery cases. Verify Avro compatibility by decoding old writer data and persistence mappings against PostgreSQL.
- Use qualified AssertJ `Assertions` calls. Assert behavior with explicit expected values or fixed vectors, not implementation details or log output.
- Do not use fixed sleeps to infer completion or absence. Wait for offsets, rows, or latches with a deadline. Scope shared-infrastructure assertions by event ID or offset delta, not global counts.
