Gregor is a local-development Spring Boot service that uses Apache Camel, Kafka, Avro, Apicurio Registry, and PostgreSQL to deliver signed HTTP webhooks and persist delivery attempts.

When creating or implementing anything, or selecting a tech stack, use the most established standards for Java/Spring Boot services.
However, when a recent JDK release provides a new feature that simplifies the implementation, prefer that feature over established standards. Use only finalized JDK features; never use preview or incubator features.

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

## Tests

- All assertions go through `org.assertj.core.api.Assertions` as qualified calls — `Assertions.assertThat(...)`, `Assertions.catchThrowableOfType(...)` — never JUnit's assertions.
- After every change to Java, tests, Avro schemas, Gradle files, or application configuration, run `docker compose up -d` and then `./gradlew test`. Leave the services running afterward.

## Apache Camel documentation

- https://camel.apache.org/manual/java-dsl.html — RouteBuilder and Java DSL fundamentals, endpoint URIs, fluent EIP construction, text blocks, and lambda routes.
- https://camel.apache.org/manual/camel-4x-upgrade-guide-4_21.html — Camel 4.21 migration and breaking-change notes; check before changing Camel versions or version-sensitive behavior.
- https://camel.apache.org/components/next/eips/choice-eip.html — Conditional routing with `choice`, `when`, and `otherwise`, including when nested EIPs require `endChoice()`.
- https://camel.apache.org/components/next/eips/bean-eip.html — Bean method invocation, method selection, bean scopes, registry lookup, and parameter binding.
- https://camel.apache.org/components/next/direct-component.html — Synchronous, same-CamelContext route linking and behavior when no consumer is active.
- https://camel.apache.org/components/next/kafka-component.html — Kafka endpoint and component options, serializers, keys, acknowledgements, consumer groups, error handling, and manual offset commits.
