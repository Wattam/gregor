# Apache Camel Bean EIP

Use the Bean EIP to invoke application code from a route.
The returned value becomes the message body. A `void` method leaves the current body in place.

Upstream sources:

- https://camel.apache.org/components/next/eips/bean-eip.html
- https://camel.apache.org/manual/bean-binding.html
- https://camel.apache.org/manual/parameter-binding-annotations.html

## Bean selection

Use one of these Java DSL forms:

```java
.bean(this.webhookService, "sendWebhookEvent")
.bean("webhookService", "sendWebhookEvent")
.bean(WebhookService.class, "sendWebhookEvent")
```

An object reference invokes that object. A string looks up the bean ID in the Camel registry.
A class lets Camel create or find an instance according to the active registry and scope.

Spring beans are available through the Camel registry in a Spring Boot application.
Prefer constructor injection and an object reference in Gregor routes.

The component URI form is also valid:

```text
bean:webhookService?method=sendWebhookEvent
```

Prefer `.bean(...)` in Java routes. It gives compile-time types for the bean reference.
It also follows the current route style.

## Method selection

Specify the method name when a bean has more than one public method:

```java
.bean(this.webhookService, "createAttempt")
```

If no method is specified, Camel selects the best matching method from the message body and available binding data.
This selection can become ambiguous after a method overload or a body type change.

Use a type qualifier to select an overload:

```java
.bean(this.exampleService, "handle(String.class)")
.bean(this.exampleService, "handle(com.example.Order.class)")
```

Use `*` as a wildcard parameter:

```java
.bean(this.exampleService, "handle(*, boolean.class true)")
```

Camel 4 supports a declared parameter type and value in the same method option:

```java
.bean(this.exampleService, "handle(String.class ${body}, boolean.class ${header.highPriority})")
```

Avoid overloaded route methods when a clear method name removes the need for type qualifiers.

## Parameter binding

Camel binds method parameters from the current exchange.
It applies Camel type conversion when the source and target types differ.

Use annotations for explicit binding:

```java
public DeliveryAttempt createAttempt(
        @Body WebhookRouteMessage routeMessage,
        @Header("correlationId") String correlationId,
        Exchange exchange
) {
    // Build and return the attempt.
}
```

Common inputs are:

- `@Body` for the message body.
- `@Header("name")` for one message header.
- `@Headers` for the message header map.
- `@ExchangeProperty("name")` for one exchange property.
- `Exchange` for the full exchange.
- `Message` for the Camel message.

Use the `org.apache.camel` binding annotations and types.
Use annotations when a method has several bindable values or when its source is not clear from the parameter type.

## Method-option values

The method option accepts these value forms:

- `true` or `false` for a boolean.
- A number such as `4`.
- A quoted string such as `'retry'`.
- `null` for a null value.
- `*` for automatic binding.
- A Simple expression such as `${body}`, `${headers}`, or `${header.retryCount}`.
- A type qualifier that ends in `.class`.

Example:

```java
.bean(this.exampleService, "handle(${body}, ${header.retryCount}, 'retry')")
```

Keep method-option expressions small. Put complex value construction in a typed Java method.

## Bean scope

The default scope is `Singleton`. Camel creates or looks up one bean and reuses it.
The bean must be safe for concurrent calls.

`Request` scope uses one bean for one exchange. Calls from the same exchange use that bean.

`Prototype` scope requests a bean for each call.
The Spring or CDI registry controls the actual lifecycle of a looked-up bean.

Gregor services are Spring singleton beans. Do not store exchange-specific mutable state in service fields.

## Bean predicates

Use a method as an expression or predicate:

```java
.choice().when().method(this.webhookService, "shouldRetry")
```

Camel binds the method parameters with the same bean-binding rules.
Camel converts the return value to the required predicate type.
Prefer a `boolean` return type for predicate methods.
