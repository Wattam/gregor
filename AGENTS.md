## Scope

- Document only current behavior in `README.md`. Omit roadmap references and future work.
- Always treat formatting-only changes as intentional.
- Never use deprecated APIs, dependencies, commands, or configuration options. Use supported replacements.

## Package structure

Application code lives under `gregor` (`src/main/java/gregor`).

```text
gregor
├── avro                 Kafka record types, Avro codecs, and registry schema resolution
├── component            Spring helpers for outbound HTTP and webhook signing
├── config               Spring bean wiring, Kafka topic declarations, and scheduling
│   └── properties       Typed configuration bindings
├── controller           HTTP endpoints
│   └── advice           HTTP exception-to-response mapping
├── dto                  HTTP request and response types, internal route messages and results
├── entity               JPA mappings
├── exception            Application exceptions
├── repository           JPA and JDBC database access
├── route                Camel message flow and Kafka offset commits
└── service              Delivery and retry policy, dead-letter handling, and persistence coordination
```

## Java

- Prefer newer finalized JDK features when they simplify the code. Never use preview or incubator features.
- Name fields, parameters, and locals after their declared type (`ObjectMapper objectMapper`). If several variables share a type, prefix their roles (`receivedWebhookEvent`, `attemptedWebhookEvent`). Apply the same rule to constants in uppercase snake case (`TEST_RECEIVER`).
- Use plural names for collections, maps, arrays, and similar containers (`List<WebhookEvent> webhookEvents`). Use descriptive names for primitives, boxed primitives, `String`, `Object`, and value types such as `UUID`, `Instant`, and `Duration`.
- Limit lines to 240 columns. Keep arguments and parameters on one line or put exactly one per line. Put `)` on its own line when wrapping the list, not when only a lambda body wraps.
- Keep a method chain on one line or put each chained call on its own line, breaking before the leading dot. Keep the receiver or initial call on the first line (`Assertions.assertThat(...)`). Camel EIPs follow the rule below.
- In wrapped map factory calls, keep each key and value together. Put exactly one pair per line.
- Keep array initializers on one line or put exactly one element per line with `}` on its own line. For a wrapped `@ValueSource` array, keep `{` on the annotation line and close with `})`.
- Qualify instance members with `this.` or `super.`. Qualify external statics with their declaring class; leave own statics unqualified. Never use static imports.
- For string and character checks other than equality, use non-deprecated `org.apache.commons.lang3.StringUtils` methods such as `isBlank` and `isEmpty`. For checks moved to `org.apache.commons.lang3.Strings`, use `Strings.CS` for case-sensitive checks (`startsWith`, `contains`) and `Strings.CI` for case-insensitive variants.
- Use `Objects.equals` for non-primitive equality; negate it for inequality.
- Use `Objects.isNull` and `Objects.nonNull` for null checks.
- Prefer Lombok annotations over handwritten constructors, getters, setters, and other boilerplate.
- Use Lombok `@Slf4j` with bracketed fields: `log.info("Published event [id={}]", eventId)`.
- Format Camel routes as EIP trees between `// @formatter:off` and `// @formatter:on`. Indent nested steps four spaces, align `.end()` with its opener, and keep each EIP's calls together (`.choice().when().method(...)`).
- In concrete Camel route classes, declare only `configure()` and methods that take a single `Exchange` parameter.
- Use Jackson 3 (`tools.jackson.databind`), not transitive Jackson 2. Keep core annotations under `com.fasterxml.jackson.annotation`. Inject the configured mapper in production.

## Configuration and tests
- Run `docker compose up -d` before integration tests that use real Kafka, PostgreSQL, or Apicurio.
- After changing a persisted column, run `docker compose down -v` before testing.
- Add metadata for every `camel.*` key in `application.yaml` to `src/main/resources/META-INF/additional-spring-configuration-metadata.json`; Camel does not supply it.
- Cover status and validation matrices in focused tests. Use full-stack tests for representative wiring and recovery cases. Verify Avro compatibility by decoding old writer data and persistence mappings against PostgreSQL.
- Use qualified AssertJ `Assertions` calls. Assert behavior with explicit expected values or fixed vectors, not implementation details or log output.
- Do not use fixed sleeps to infer completion or absence. Wait for offsets, rows, or latches with a deadline. Scope shared-infrastructure assertions by event ID or offset delta, not global counts.
