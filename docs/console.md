# Eventify Console

Eventify Console is an operations-focused UI for inspecting and troubleshooting applications built with Eventify.

Explore aggregates, replay their state at any point in history, inspect event payloads and state diffs, trace commands through correlation and causation, and retry failed commands — all from a single interface.

The console runs as its own Docker container. Your applications connect to it: they show up by themselves as soon as they start, and they don't need to open a port.

## How it works

```
Browser ──HTTP──▶ Eventify Console (Docker) ◀──RSocket── your application instances
```

- Every application instance opens a connection to the console (RSocket over WebSocket) and tells it who it is.
- The browser only talks to the console. The console passes each request on to an instance of the chosen application, over that instance's own connection.
- Queries about an aggregate are answered by the instance that owns it. When the console asks another instance, that instance names the owner, and the console asks the owner instead.

Because the applications open the connection:

- the applications don't need to be reachable from the console or from the browser;
- there is no list of application URLs to maintain: new applications and instances appear automatically.

## Running the console

```bash
docker run -p 8080:8080 ghcr.io/alikelleci/eventify-console:latest
```

The console is then available at `http://localhost:8080/console/`. Until an application connects, it shows how to connect one.

## Connecting an application

Add the plugin to your application:

```xml
<dependency>
    <groupId>io.github.alikelleci</groupId>
    <artifactId>eventify-console-plugin</artifactId>
    <version>x.y.z</version>
</dependency>
```

Register it with the console's address, the same one you open in the browser:

```java
Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-app");
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

Eventify eventify = Eventify.builder()
    .streamsConfig(props)
    .registerPlugin(EventifyConsolePlugin.builder()
        .url("http://eventify-console:8080")
        .build())
    .build();

eventify.start();
```

The application appears in the console under its `application.id`. All instances with the same `application.id` are one application.

When the console is unreachable, or the connection drops (for example while the console is redeployed), the plugin keeps reconnecting in the background, waiting up to 30 seconds between attempts. Your application keeps running normally either way.

If the console refuses the application, for example because it doesn't support the application's protocol version yet, the plugin logs the reason once and tries again every 30 seconds.

### Plugin options

| Method | Required | Description |
|---|---|---|
| `url(String)` | Yes | The console's address, e.g. `http://eventify-console:8080`. Use `https://` when the console is served over TLS. |

### Spring Boot

With the Spring Boot starter, register the plugin on your `Eventify` bean, the same way:

```java
@Bean
public Eventify eventify() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-app");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    return Eventify.builder()
        .streamsConfig(props)
        .registerPlugin(EventifyConsolePlugin.builder()
            .url("http://eventify-console:8080")
            .build())
        .build();
}
```

The console's address usually differs per environment, so you'll typically read it from your own application configuration.

## `application.server`

Eventify sets Kafka Streams' `application.server` to a unique name for each instance, like `my-app.3f2a9c1e-7b4d-4c1a-9f0e-5d8a2b6c1e44:0`. It is not an address: nothing listens on it. Kafka Streams shares it between the instances, so every instance knows which one owns an aggregate.

If you set `application.server` yourself, for example for your own interactive queries, Eventify keeps your value and the console uses it as the instance's name. It must be unique per instance.

## Configuring the console

The console is a Spring Boot application, so it can be configured with environment variables:

| Variable | Default | Description |
|---|---|---|
| `SERVER_PORT` | `8080` | The port for the UI, the API and the connections from the applications. |
| `EVENTIFY_CONSOLE_REQUESTTIMEOUT` | `60s` | How long to wait for an application to answer. Reading commands from Kafka can take a while. |

## Running more than one console

Run one console per environment (for example one for test, one for production), and point the applications of that environment at it.

Within an environment, run a single console instance. It keeps the connected applications in memory; after a restart, the applications connect again within seconds.

## Security

The console does not implement authentication yet. Do not expose it publicly. Place it behind a reverse proxy or load balancer that handles authentication, and only allow your applications to reach its `/rsocket` endpoint.

Anyone who can reach the console can see the events of the connected applications and retry commands.
