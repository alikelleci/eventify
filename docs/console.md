# Eventify Console

Eventify Console is an operations-focused UI for inspecting and troubleshooting applications built with Eventify.

Explore aggregates, replay their state at any point in history, inspect event payloads and state diffs, trace commands through correlation and causation, and retry failed commands — all from a single interface.

The console runs as its own Docker container. Your applications connect to it: they show up by themselves as soon as they start, and they don't need to open a port.

## Running the console

```bash
docker run -p 8080:8080 ghcr.io/alikelleci/eventify-console:latest
```

The console is available at `http://localhost:8080`.

## Connecting an application

Add the plugin to your application:

```xml
<dependency>
    <groupId>io.github.alikelleci</groupId>
    <artifactId>eventify-console-plugin</artifactId>
    <version>x.y.z</version>
</dependency>
```

Register it with the console's address:

```java
Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-app");
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

Eventify eventify = Eventify.builder()
    .streamsConfig(props)
    .registerPlugin(EventifyConsolePlugin.builder()
        .url("http://localhost:8080")
        .build())
    .build();

eventify.start();
```

### Plugin options

| Method | Required | Description |
|---|---|---|
| `url(String)` | Yes | The console's address, e.g. `http://localhost:8080`. Use `https://` when the console is served over TLS. |
| `token(String)` | No | The console's application token (see [Security](#security)). |

## Configuring the console

The console can be configured with following environment variables:

| Variable | Default | Description |
|---|---|---|
| `SERVER_PORT` | `8080` | The port for the UI, the API and the connections from the applications. |
| `EVENTIFY_CONSOLE_REQUESTTIMEOUT` | `60s` | How long to wait for an application to answer. Reading commands from Kafka can take a while. |
| `EVENTIFY_CONSOLE_APPTOKEN` | – | The token applications must send to connect. See [Security](#security). |
| `EVENTIFY_CONSOLE_OIDC_ISSUERURI` | – | Your identity provider, e.g. `https://login.example.com/realms/ops`. Turns on login. |
| `EVENTIFY_CONSOLE_OIDC_CLIENTID` | – | The console's client id at the identity provider. |
| `EVENTIFY_CONSOLE_OIDC_CLIENTSECRET` | – | The console's client secret at the identity provider. |

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

### Application token

Without a token, any client that can reach the console can connect as an application. To prevent this, start the console with a secret token:

```bash
docker run -p 8080:8080 -e EVENTIFY_CONSOLE_APPTOKEN=... ghcr.io/alikelleci/eventify-console:latest
```

```java
EventifyConsolePlugin.builder()
    .url("http://eventify-console:8080")
    .token(System.getenv("EVENTIFY_CONSOLE_TOKEN"))
    .build()
```
