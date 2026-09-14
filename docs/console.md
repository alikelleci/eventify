# Eventify Console

Eventify Console is an operations-focused UI for inspecting and troubleshooting applications built with Eventify.

Explore aggregates, replay their state at any point in history, inspect event payloads and state diffs, trace commands through correlation and causation, and retry failed commands — all from a single interface.

Available both embedded in your application or as a standalone Docker deployment for centralized operations.

It consists of two optional modules:

- `eventify-console-server` — embeds an HTTP server into your application that exposes the API and serves the UI
- `eventify-console-ui` — the Angular UI, bundled as a jar and served by the console server

Both are opt-in. Include them only if you want the console.

## Installation

Add both modules to your project:

```xml
<dependency>
    <groupId>io.github.alikelleci</groupId>
    <artifactId>eventify-console-server</artifactId>
    <version>x.y.z</version>
</dependency>
<dependency>
    <groupId>io.github.alikelleci</groupId>
    <artifactId>eventify-console-ui</artifactId>
    <version>x.y.z</version>
</dependency>
```

## Configuration

The console server binds to the host and port declared in `application.server`. This property serves two purposes: it tells Kafka Streams where this node can be reached for inter-node state queries, and it determines the address the console server listens on.

```java
Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-app");
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:8085");

Eventify eventify = Eventify.builder()
    .streamsConfig(props)
    .registerPlugin(EventifyConsolePlugin.builder().build())
    .build();

eventify.start();
```

Once started, the console is available at:

```
http://localhost:8085/console/
```

### Plugin options

| Method | Required | Description |
|---|---|---|
| `allowedOrigins(String)` | No | CORS allowed origins. Defaults to `*`. |

## Spring Boot Integration

When using the Spring Boot starter, the console plugin is registered automatically if `eventify-console-server` is on the classpath. No explicit plugin registration is needed — only `application.server` must be set.

```java
@Bean
public Eventify eventify() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-app");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:8085");

    return Eventify.builder()
        .streamsConfig(props)
        .build();
}
```

## Deployment Modes

### Embedded

The UI is served directly by your application. Each application gets its own console at its own host and port. This is the default mode when the jars are on the classpath.

### Standalone Docker

For a centralized view across multiple applications, a standalone Docker image is available. It serves the same UI but allows you to configure multiple application URLs and switch between them.

```bash
docker run -p 8080:80 \
  -v /path/to/config.yaml:/etc/eventify/config.yaml \
  ghcr.io/alikelleci/eventify-console:latest
```

The console is then available at `http://localhost:8080/console/`.

#### config.yaml

```yaml
eventify:
  apps:
    - name: My App 1
      url: http://localhost:8085
    - name: My App 2
      url: http://localhost:8086
```

The console runs in your browser and calls these URLs directly, so:

- each URL must be reachable from the machine where the browser runs, not only from inside the container;
- the applications must allow the console's origin through CORS (`allowedOrigins`, which defaults to `*`);
- when the console is served over HTTPS, the application URLs must use HTTPS as well, or the browser blocks the calls.

The config file location can be changed with the `EVENTIFY_CONFIG` environment variable. Without a config file, the console shows how to configure one.

## Security

The console server does not implement authentication. For production deployments, do not expose the console port publicly. Place it behind a reverse proxy or load balancer that handles authentication.
