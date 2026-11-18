Gregor is a local-development Spring Boot service that uses Apache Camel, Kafka, Avro, Apicurio Registry, and PostgreSQL to deliver signed HTTP webhooks and persist delivery attempts.

When creating or implementing anything, or selecting a tech stack, use the most established standards for Java/Spring Boot services.
However, when a recent JDK release provides a new feature that simplifies the implementation, prefer that feature over established standards. Use only finalized JDK features; never use preview or incubator features.

## Scope & documentation

- `IMPLEMENTATION-PHASES.md` defines what belongs to the increment being implemented. Read it before starting work, and update the phase status when an increment lands.
- `README.md` documents only the actual current state of the project. Never reference `IMPLEMENTATION-PHASES.md`, planned phases, or future work from it.

## Source layout & style

- Maximum line length is 120. When a call does not fit, either break after `=` and indent the whole call onto the next line, or split it one argument per line with the closing `)` on its own new line.
  Method chains break before the leading `.`.
- Access instance members through `this.` (`this.bean`, `this.method(...)`); use `super.` for members inherited from the superclass.
- Reference statics through their declaring class when accessing them from another class (`Mockito.mock(...)`, `StandardCharsets.UTF_8`).
  When accessing a static method, variable, or class declared by the current class, omit the class qualifier (`run(...)`, `MAXIMUM_ATTEMPTS`, `NestedClass`). Never use static imports.
- Log through the Slf4j `log` field with bracketed placeholders: `log.info("Clipping field boundary [z={}] [x={}] [y={}]", z, x, y)`. Add the field with Lombok's `@Slf4j`.
- Format Camel route definitions as EIP trees inside `// @formatter:off` and `// @formatter:on`: indent nested EIPs by four spaces, align steps in the same scope, and align each `.end()` with the EIP it closes.
  Keep calls forming one EIP on the same line, such as `.multicast().stopOnException()` and `.choice().when().method(...)`.
- Apply these Java style rules to production and test code.
- Never edit generated Avro Java. Edit the source `.avsc` schemas and regenerate it through Gradle.
- Generated Avro records have no setters (`createSetters = false`). Derive a changed record with `WebhookEvent.newBuilder(existing).setDeliveryAttemptCount(...).build()`.
- Jackson is version 3. Take `ObjectMapper`, `JsonMapper`, `JsonNode`, and node factories from `tools.jackson.databind`, and databind annotations (`@JsonSerialize`, `@JsonDeserialize`, `@JsonNaming`) from `tools.jackson.databind.annotation`.
  Jackson 3 ships no annotation package of its own, so the core annotations (`@JsonProperty`, `@JsonInclude`, `@JsonCreator`) still come from `com.fasterxml.jackson.annotation`.
  Never import `com.fasterxml.jackson.databind`: Jackson 2 is on the compile classpath transitively, so the wrong mapper compiles and fails only at runtime.
- Production code injects the `@Primary JsonMapper` bean declared by `JacksonConfiguration` instead of constructing a mapper.

## Configuration & persistence

- The Camel starter publishes no configuration metadata. Every `camel.*` property used in `application.yaml` needs a matching entry in `src/main/resources/META-INF/additional-spring-configuration-metadata.json`.
- There are no database migrations: `ddl-auto: update` derives the schema from the JPA entities. After changing a persisted column, recreate the volumes with `docker compose down --volumes` and `docker compose up -d` before running tests.

## Tests

- Test each behavior at the cheapest layer that can prove it. Put status and validation matrices in focused tests; use representative cases in full-stack tests to prove HTTP, Kafka, or PostgreSQL wiring and failure recovery.
- Assert observable behavior and published contracts. Do not pin annotations, generated accessors, private fields, bean names, or Camel route counts. Verify Avro compatibility by decoding old writer data and persistence mappings by writing to PostgreSQL.
- Use explicit expected values or fixed test vectors. Do not copy production branches into assertions or use the production signer to generate the expected digest in its own test.
- Do not use fixed sleeps to infer completion or absence. Wait for a Kafka offset, a persisted row, or a latch with a deadline. On shared infrastructure, scope assertions by event ID or compare broker offsets before and after the action instead of asserting global receiver counts.
- All assertion API calls go through `org.assertj.core.api.Assertions` as qualified calls — `Assertions.assertThat(...)`, `Assertions.catchThrowableOfType(...)` — never JUnit's assertion API. Tests can throw `AssertionError` directly for custom failure paths.
- Do not test log text or assert on log output. Do not use `CapturedOutput` or `OutputCaptureExtension`. Tests can verify that a method whose effect is logging was called.
- After every change to Java, tests, Avro schemas, Gradle files, or application configuration, run `docker compose up -d` and then `./gradlew test`. Leave the services running afterward.

## Apache Camel documentation

- `docs/apache-camel/java-dsl.md` — RouteBuilder and Java DSL fundamentals, endpoint URIs, fluent EIP construction, text blocks, and lambda routes.
- `docs/apache-camel/camel-4.21-upgrade-guide.md` — Camel 4.21 migration and breaking-change notes. Check it before a Camel version change or a version-sensitive behavior change.
- `docs/apache-camel/choice-eip.md` — Conditional routing with `choice`, `when`, and `otherwise`. It includes the use of `endChoice()` with nested EIPs.
- `docs/apache-camel/bean-eip.md` — Bean method invocation, method selection, bean scopes, registry lookup, and parameter binding.
- `docs/apache-camel/direct-component.md` — Synchronous route links in one CamelContext and behavior when no consumer is active.
- `docs/apache-camel/kafka-component.md` — Kafka endpoints, serializers, keys, acknowledgements, consumer groups, error handling, and manual offset commits.
