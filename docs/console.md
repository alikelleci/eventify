# Eventify Console

Eventify Console is an operations-focused UI for inspecting and troubleshooting applications built with Eventify.

Explore aggregates, replay their state at any point in history, inspect event payloads and state diffs, trace commands through correlation and causation, and retry failed commands — all from a single interface.

The console runs as its own Docker container. Your applications connect to it: they show up by themselves as soon as they start, and they don't need to open a port.

## Running the console

```bash
docker run -p 8080:8080 ghcr.io/alikelleci/eventify-console:latest
```

The console is available at `http://localhost:8080/console/`.

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
| `token(String)` | When the console requires it | The console's application token (see [Security](#security)). Read it from the environment, e.g. `System.getenv("EVENTIFY_CONSOLE_TOKEN")`, rather than putting it in code. |

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
| `EVENTIFY_CONSOLE_APPTOKEN` | – | The secret applications must send to connect. See [Security](#security). |
| `EVENTIFY_CONSOLE_OIDC_ISSUERURI` | – | Your identity provider, e.g. `https://login.example.com/realms/ops`. Turns on login. |
| `EVENTIFY_CONSOLE_OIDC_CLIENTID` | – | The console's client id at the identity provider. |
| `EVENTIFY_CONSOLE_OIDC_CLIENTSECRET` | – | The console's client secret at the identity provider. |

## Running more than one console

Run one console per environment (for example one for test, one for production), and point the applications of that environment at it.

Within an environment, run a single console instance. It keeps the connected applications in memory; after a restart, the applications connect again within seconds.

## Security

Two things protect the console: people log in with your identity provider, and applications prove who they are with a token. Configure both in production. Without them, the console logs a warning at startup.

### Login

The console supports login with any OpenID Connect identity provider, such as Keycloak, Microsoft Entra ID, Okta or Google.

1. Register the console as a client at your identity provider, with this redirect URI:
   ```
   https://<console address>/login/oauth2/code/sso
   ```
2. Start the console with the provider and the client's credentials:
   ```bash
   docker run -p 8080:8080 \
     -e EVENTIFY_CONSOLE_OIDC_ISSUERURI=https://login.example.com/realms/ops \
     -e EVENTIFY_CONSOLE_OIDC_CLIENTID=eventify-console \
     -e EVENTIFY_CONSOLE_OIDC_CLIENTSECRET=... \
     ghcr.io/alikelleci/eventify-console:latest
   ```

Every page and API call then needs a login. The console shows who is logged in, with a button to log out. The identity provider must be reachable when the console starts, because the console reads its settings then.

Behind a proxy that terminates TLS, make sure the proxy sends the `X-Forwarded-*` headers, so the redirects use the address people opened.

Your applications don't log in: they use the same console address as people, and prove who they are with the application token. If you use a login in front of the console instead, for example on a load balancer or proxy, your applications can't pass it: give them an address that doesn't go through that login, such as the console's internal address, or exclude the path `/rsocket` from that login.

### Application token

Without a token, any client that can reach the console can connect as an application, and could show made-up data or receive retries. To prevent that, start the console with a secret token, and give every application the same token:

```bash
docker run -p 8080:8080 -e EVENTIFY_CONSOLE_APPTOKEN=... ghcr.io/alikelleci/eventify-console:latest
```

```java
EventifyConsolePlugin.builder()
    .url("http://eventify-console:8080")
    .token(System.getenv("EVENTIFY_CONSOLE_TOKEN"))
    .build()
```

The console refuses applications without the right token; they log the reason and try again every 30 seconds. Without `EVENTIFY_CONSOLE_APPTOKEN`, the console accepts every application and logs a warning at startup.
