# Apache Camel Java DSL

This file is the project reference for Camel routes that use the Java DSL.

Upstream source: https://camel.apache.org/manual/java-dsl.html

## Route definition

Extend `org.apache.camel.builder.RouteBuilder`. Implement `configure()` to build the route model.
Camel calls this method when it adds the builder to the `CamelContext`.

A route starts with one consumer endpoint in `from(...)`. It then contains processors and producer endpoints.

```java
@Override
public void configure() {
    from("direct:input")
        .routeId("example-route")
        .bean(this.exampleService, "prepare")
        .to("direct:output");
}
```

`configure()` defines the route. It does not process an exchange.

Use a stable and unique route ID. Gregor uses the source URI as the route ID through `BaseRoute.routeDefinition(...)`.

## Endpoint URIs

Use this general URI form:

```text
scheme:path?option=value&otherOption=otherValue
```

The scheme selects the component. The path identifies the component resource. Query parameters configure the endpoint.

Examples:

```text
direct:webhook-delivery-attempt
kafka:webhook-events?groupId=webhook-events
```

Set shared component options in `application.yaml`. Set route-specific options in the endpoint URI.
Use property placeholders for values that change between environments:

```java
from("kafka:{{gregor.kafka.source-topic}}?groupId={{gregor.kafka.source-group}}")
    .to("direct:process-event");
```

A placeholder uses the `{{property.name}}` form in a Camel URI.

## Java text blocks

Use a Java text block for a long URI. Put one query parameter on each line.

```java
from("""
        kafka:webhook-events
        ?groupId=webhook-events
        &autoOffsetReset=earliest
        """)
    .to("direct:webhook-delivery-attempt");
```

Use this form only when it improves the route. Prefer component configuration for shared options.

## EIP scope

The Java DSL builds a typed tree. An EIP can contain child EIPs.
Close each child scope before work continues in its parent scope.

Use `.end()` to close an EIP scope.
Use `.endChoice()` when a nested EIP changes the Java builder type inside a choice branch.
See [Choice EIP](choice-eip.md).

Gregor formats route definitions as EIP trees:

```java
// @formatter:off
from("direct:input")
    .choice().when().method(this.exampleService, "isValid")
        .bean(this.exampleService, "process")
    .otherwise()
        .bean(this.exampleService, "reject")
    .end();
// @formatter:on
```

Keep one EIP call on one line. Examples are `.choice().when().method(...)` and `.multicast().stopOnException()`.
Indent child EIPs by four spaces. Align `.end()` with the EIP that it closes.

## Message flow

A Camel exchange contains the message body, headers, exchange properties, and processing state.
Most processors change the same exchange and pass it to the next processor.

Use these common Java DSL operations:

- `.bean(bean, method)` invokes application code.
- `.process(processor)` gives application code access to the full exchange.
- `.setBody(expression)` replaces the body.
- `.setHeader(name, expression)` sets one message header.
- `.removeHeaders(pattern)` removes matching message headers.
- `.to(uri)` sends the exchange to a producer endpoint.
- `.choice()` selects one branch.
- `.multicast()` sends copies to child routes.
- `.pipeline()` runs child processors in order.

Do not assume that transport headers are safe for another transport. Remove or map headers at transport boundaries.

## Lambda route builder

Camel also supports `LambdaRouteBuilder`:

```java
rb -> rb.from("kafka:webhook-events").to("direct:webhook-delivery-attempt");
```

Gregor uses `RouteBuilder`. Keep this form unless a complete route is simpler with the lambda API.
Do not mix route styles without a clear need.
