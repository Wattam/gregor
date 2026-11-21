## Scope

- Document only current behavior in `README.md`. Omit roadmap references and future work.
- Always treat formatting-only changes as intentional.

## Java

- Prefer newer finalized JDK features when they simplify the code. Never use preview or incubator features.
- Name fields, parameters, and locals after their declared type (`ObjectMapper objectMapper`). If several variables share a type, prefix their roles (`receivedWebhookEvent`, `attemptedWebhookEvent`). Apply the same rule to constants in uppercase snake case (`TEST_RECEIVER`).
- Use plural names for collections, maps, arrays, and similar containers (`List<WebhookEvent> webhookEvents`). Use descriptive names for primitives, boxed primitives, `String`, `Object`, and value types such as `UUID`, `Instant`, and `Duration`.
- Limit lines to 240 columns. Keep arguments and parameters on one line or put exactly one per line. Put `)` on its own line when wrapping the list, not when only a lambda body wraps.
- Keep a method chain on one line or put each chained call on its own line, breaking before the leading dot. Keep the receiver or initial call on the first line (`Assertions.assertThat(...)`). Camel EIPs follow the rule below.
- In wrapped map factory calls, keep each key and value together. Put exactly one pair per line.
- Keep array initializers on one line or put exactly one element per line with `}` on its own line. For a wrapped `@ValueSource` array, keep `{` on the annotation line and close with `})`.
- Qualify instance members with `this.` or `super.`. Qualify external statics with their declaring class; leave own statics unqualified. Never use static imports.
- Use `org.apache.commons.lang3.StringUtils` for string and character checks (`isBlank`, `isEmpty`, etc.), except equality.
- Use `Objects.equals` for non-primitive equality; negate it for inequality.
- Use `Objects.isNull` and `Objects.nonNull` for null checks.
- Prefer Lombok annotations over handwritten constructors, getters, setters, and other boilerplate.
- Use Lombok `@Slf4j` with bracketed fields: `log.info("Published event [id={}]", eventId)`.
- Format Camel routes as EIP trees between `// @formatter:off` and `// @formatter:on`. Indent nested steps four spaces, align `.end()` with its opener, and keep each EIP's calls together (`.choice().when().method(...)`).
- Use Jackson 3 (`tools.jackson.databind`), not transitive Jackson 2. Keep core annotations under `com.fasterxml.jackson.annotation`. Inject the configured mapper in production.

## Configuration and tests
- Run `docker compose up -d` before integration tests that use real Kafka, PostgreSQL, or Apicurio.
- After changing a persisted column, run `docker compose down -v` before testing.
- Add metadata for every `camel.*` key in `application.yaml` to `src/main/resources/META-INF/additional-spring-configuration-metadata.json`; Camel does not supply it.
- Cover status and validation matrices in focused tests. Use full-stack tests for representative wiring and recovery cases. Verify Avro compatibility by decoding old writer data and persistence mappings against PostgreSQL.
- Use qualified AssertJ `Assertions` calls. Assert behavior with explicit expected values or fixed vectors, not implementation details or log output.
- Do not use fixed sleeps to infer completion or absence. Wait for offsets, rows, or latches with a deadline. Scope shared-infrastructure assertions by event ID or offset delta, not global counts.
