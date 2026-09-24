# Eventify Console

Eventify Console is an ops UI for inspecting aggregates, events and command outcomes.

## Run it

```bash
docker run -p 8080:8080 ghcr.io/alikelleci/eventify-console:latest
```

Open `http://localhost:8080`.

## Connect an application

Add the client dependency and register the plugin.

```xml
<dependency>
    <groupId>io.github.alikelleci</groupId>
    <artifactId>eventify-console-client</artifactId>
    <version>x.y.z</version>
</dependency>
```

```java
Eventify eventify = Eventify.builder()
    .streamsConfig(config)
    .registerPlugin(EventifyConsoleClient.builder()
        .url("http://localhost:8080")
        .build())
    .build();
```

Connected applications appear automatically. The console can show stored events, aggregate state and command results.

## Secure it

For production, protect both the UI and application connection:

- Set `EVENTIFY_CONSOLE_APPTOKEN` on the console and pass the same value with `.token(...)` in each client.
- Configure OpenID Connect with `EVENTIFY_CONSOLE_OIDC_ISSUERURI`, `EVENTIFY_CONSOLE_OIDC_CLIENTID` and `EVENTIFY_CONSOLE_OIDC_CLIENTSECRET`.

The redirect URI is `https://<console-address>/login/oauth2/code/sso`.
