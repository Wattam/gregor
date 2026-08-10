# Apache Camel 4.21 upgrade guide

This file contains the Camel 4.21 changes that apply to Gregor.
Read the complete upstream guide before a Camel version change.

Upstream source: https://camel.apache.org/manual/camel-4x-upgrade-guide-4_21.html

## Current project version

`build.gradle` imports `camel-spring-boot-bom:4.21.0`.
It also declares `camel-test-spring-junit5:4.21.0`.
Keep the BOM and explicit Camel test dependencies on the same version.

## Kafka and Spring Boot property bridge

Camel 4.21 automatically bridges supported `spring.kafka.*` settings to the Camel Kafka component.
The bridged settings include these groups:

- Bootstrap servers.
- Security protocol.
- TLS key store and trust store.
- Consumer group ID.
- Client ID.
- SASL mechanism, JAAS configuration, and Kerberos service name.

Explicit `camel.component.kafka.*` settings take precedence over bridged Spring settings.

Gregor configures Spring Kafka for topic administration. It configures Camel Kafka for route consumers and producers.
After an upgrade, verify which value each client receives. Do not remove explicit Camel route settings without a test.

Disable the bridge with this property only when the two clients need different supported values:

```properties
camel.component.kafka.bridge-spring-kafka-properties=false
```

## Component documentation layout

Camel 4.21 removes Spring Boot auto-configuration tables from individual component pages.
The Camel Spring Boot documentation now contains one starter configuration page per component.

Use component pages for endpoint behavior. Use starter pages for `camel.component.*` property names and types.
Gregor must still add configuration metadata for every `camel.*` property in `application.yaml`.

## Header filtering

`DefaultHeaderFilterStrategy` now uses lowercase matching by default.
Header name matching is therefore case-insensitive unless code calls `setLowerCase(false)`.

The strategy now blocks headers that start with `Camel` in either letter case in both transport directions.
This applies directly to custom strategies that configure or extend `DefaultHeaderFilterStrategy`.

Use Camel constants for framework headers. Remove transport headers before a new transport publication.
Do not depend on a `Camel*` application header crossing Kafka or HTTP boundaries.

`org.apache.camel.http.common.HttpProtocolHeaderFilterStrategy` is deprecated.
Use `org.apache.camel.http.base.HttpProtocolHeaderFilterStrategy` if future code needs this class.

## Core API changes

The internal `org.apache.camel.language.simple.SimpleExpressionBuilder` class was removed.
Its methods moved to domain-specific internal builder classes.
Do not use Camel internal Simple builder classes in Gregor. Use the public Java DSL and Simple language.

Simple and CSimple behavior changed in these areas:

- `floor` and `ceil` now return `long` instead of `int`.
- CSimple `range(N)` now starts at `1`, in line with Simple.

Review route expressions before an upgrade when they use these functions.

Camel API now uses optional JSpecify null-safety annotations.
Add `org.jspecify:jspecify` directly only when a project tool must read those annotations.

Java object serialization type converters for `ObjectInput` and `ObjectOutput` were removed.
Do not restore them. Gregor uses Avro and JSON for transport data.

## Choice route model changes

`NamedNode.getChildren()` now returns structural route children such as `when` and `otherwise` nodes.
Use `getChildren()` instead of `getOutputs()` when code walks the full route model.

`when` and `otherwise` now add their own route-model level.
This changes level values in JMX and development consoles for processors inside a choice.
It does not change branch execution.

Review tests or tools that inspect route structure or processor levels.

## Error Registry changes

The Error Registry SPI changed:

- `ErrorRegistryEntry` was removed.
- `BacklogErrorEventMessage` replaces it.
- `ErrorRegistryView.browse()` returns `Collection<BacklogErrorEventMessage>`.
- Error entries always capture the full `Throwable`.
- Configuration moved from `camel.main.errorRegistry*` to `camel.errorRegistry.*`.

Gregor does not use this SPI now. Check this section before adding error-registry code.
Do not put payloads or signing secrets in captured exchange data.

## Virtual thread changes

When Camel virtual threads are enabled, `threads().maxQueueSize(...)` now applies backpressure.
Camel uses a bounded executor and uses `keepAliveTime` as the capacity-acquisition timeout.

Camel also adds the `Block` rejected policy. It waits without a time limit for capacity.
Do not use `Block` in a route that must keep Kafka polling within `maxPollIntervalMs`.

Spring virtual-thread configuration does not by itself prove that Camel EIP virtual threads are enabled.
Test the active executor and its limits before a route depends on this behavior.

## Header map dependency

`camel-headersmap` is deprecated. Camel core now provides an optimized case-insensitive header map.
Do not add `camel-headersmap` to Gregor.

## Upgrade checklist

1. Read the complete guide for 4.21 and every skipped Camel version.
2. Update the Spring Boot BOM and explicit Camel dependencies together.
3. Resolve dependencies and check for mixed Camel versions.
4. Check `spring.kafka.*` and `camel.component.kafka.*` precedence.
5. Check header filtering at each Kafka, HTTP, and other transport boundary.
6. Check Simple and CSimple expressions for changed types or ranges.
7. Check custom Camel SPI use and route-model traversal.
8. Check consumer processing time against Kafka `maxPollIntervalMs`.
9. Update local files in `docs/apache-camel` for behavior or option changes.
10. Start the Docker Compose services and run `./gradlew test`.
