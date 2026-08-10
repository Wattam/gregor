# Apache Camel Choice EIP

Use the Choice EIP for conditional routing. It implements a content-based router.

Upstream source: https://camel.apache.org/components/next/eips/choice-eip.html

## Branch selection

Camel checks `when` predicates in declaration order. It runs the first matching branch only.
Camel runs `otherwise` when no `when` predicate matches. The `otherwise` branch is optional.

```java
// @formatter:off
from("direct:input")
    .choice().when().method(this.webhookService, "hasImpossibleDeliveryState")
        .bean(this.webhookService, "logDiscardedEnvelope")
    .otherwise()
        .bean(this.webhookService, "sendWebhookEvent")
    .end();
// @formatter:on
```

Use an explicit method name for a bean predicate.
The method result must be a boolean or a value that Camel can convert to a boolean.

## Simple predicates

Use the Simple language for small message checks:

```java
// @formatter:off
from("direct:input")
    .choice().when().simple("${header.retryCount} > 0")
        .to("direct:retry")
    .otherwise()
        .to("direct:first-attempt")
    .end();
// @formatter:on
```

Use a Java method when the rule is domain logic or needs tests. Do not put complex domain rules in a Simple string.

## Closing the choice

Use `.end()` to leave the choice and continue the route:

```java
// @formatter:off
from("direct:input")
    .choice().when().simple("${body} != null")
        .to("direct:present")
    .otherwise()
        .to("direct:missing")
    .end()
    .to("direct:complete");
// @formatter:on
```

The final `.to("direct:complete")` runs after either branch.

## Nested EIPs and `endChoice()`

Some EIPs have a child route and return a different Java builder type.
Java cannot always infer a return to the choice scope.
Use `.endChoice()` to return from that nested EIP to the Choice EIP.

```java
// @formatter:off
from("direct:input")
    .choice().when().simple("${body} contains 'Camel'")
        .loadBalance().roundRobin()
            .to("direct:first")
            .to("direct:second")
        .endChoice()
    .otherwise()
        .to("direct:fallback")
    .end();
// @formatter:on
```

Use `.endChoice()` only when the nested EIP requires it.
A normal list of processors in a `when` branch does not require it.

If the fluent Java types still prevent a clear route, split the work into separate routes.
Connect the routes with `direct:` endpoints.
See [Direct component](direct-component.md).

## Project rules

- Put the most specific predicate first.
- Keep branch side effects clear.
- Test every `when` branch and the `otherwise` branch.
- Test the no-match result when there is no `otherwise` branch.
- Keep EIP tree indentation inside formatter guards.
