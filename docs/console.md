# Eventify Console

Eventify Console is an operations-focused UI for inspecting and troubleshooting applications built with Eventify.

Explore aggregates, replay their state at any point in history, inspect event payloads and state diffs, trace commands through correlation and causation, and retry failed commands — all from a single interface.

The console runs as its own Docker container. Applications connect to it directly: they appear automatically when they start.

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
| `token(String)` | When the console requires one | The console's application token. See [Application token](#application-token). |

## Configuring the console

The console is configured with the following environment variables:

| Variable | Default | Description |
|---|---|---|
| `SERVER_PORT` | `8080` | The port the console listens on, for both the UI and the application connections. |
| `EVENTIFY_CONSOLE_REQUESTTIMEOUT` | `60s` | How long to wait for an application to respond. |
| `EVENTIFY_CONSOLE_APPTOKEN` | – | The token applications must provide when connecting. See [Application token](#application-token). |
| `EVENTIFY_CONSOLE_OIDC_ISSUERURI` | – | The identity provider URL, e.g. `https://login.example.com/realms/ops`. Setting this enables login. See [Login](#login). |
| `EVENTIFY_CONSOLE_OIDC_CLIENTID` | – | The console's client ID at the identity provider. |
| `EVENTIFY_CONSOLE_OIDC_CLIENTSECRET` | – | The console's client secret at the identity provider. |

## Security

The console supports two authentication methods:

- **User authentication** through an OpenID Connect identity provider.
- **Application authentication** using a token.

For production, configure both methods. If either is not configured, the console will show a warning at startup.

### Login

The console supports any OpenID Connect identity provider, such as Keycloak, Microsoft Entra ID, Okta, or Google.

1. Register the console as a client with your identity provider using this redirect URI:
   ```
   https://<console address>/login/oauth2/code/sso
   ```
2. Start the console with the provider and client credentials:
   ```bash
   docker run -p 8080:8080 \
     -e EVENTIFY_CONSOLE_OIDC_ISSUERURI=https://login.example.com/realms/ops \
     -e EVENTIFY_CONSOLE_OIDC_CLIENTID=eventify-console \
     -e EVENTIFY_CONSOLE_OIDC_CLIENTSECRET=... \
     ghcr.io/alikelleci/eventify-console:latest
   ```

### Application token

The application token prevents unauthorized clients from connecting to the console.

Start the console with a token:
```bash
docker run -p 8080:8080 -e EVENTIFY_CONSOLE_APPTOKEN=... ghcr.io/alikelleci/eventify-console:latest
```

Configure the same token in the Eventify client:
```java
EventifyConsolePlugin.builder()
    .url("http://localhost:8080")
    .token(System.getenv("EVENTIFY_CONSOLE_TOKEN"))
    .build()
```
