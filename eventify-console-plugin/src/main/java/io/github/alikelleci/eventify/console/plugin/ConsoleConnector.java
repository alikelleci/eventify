package io.github.alikelleci.eventify.console.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.console.protocol.ConsoleProtocol;
import io.github.alikelleci.eventify.console.protocol.NodeInfo;
import io.github.alikelleci.eventify.console.protocol.ReplyHeader;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.util.DefaultPayload;
import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.function.BiFunction;

/**
 * Keeps a connection from this application instance to the Eventify Console and answers the console's requests on it.
 * The instance opens the connection, so it needs no port of its own. When the console is unreachable or the
 * connection drops, it keeps trying again in the background; the application itself is never affected.
 */
@Slf4j
public class ConsoleConnector {

  /** Both sides send a heartbeat at this interval, and close the connection after this long without one. */
  private static final Duration KEEPALIVE_INTERVAL = Duration.ofSeconds(10);
  private static final Duration KEEPALIVE_MAX_LIFETIME = Duration.ofSeconds(30);

  private static final Duration MIN_BACKOFF = Duration.ofSeconds(1);
  private static final Duration MAX_BACKOFF = Duration.ofSeconds(30);

  /** WebSocket frames are limited to 64 KB, so larger replies (a page of events) are sent in parts. */
  static final int FRAGMENT_SIZE = 16 * 1024;

  private final URI uri;
  private final NodeInfo nodeInfo;
  private final BiFunction<String, byte[], ConsoleRequestHandler.Reply> handler;
  private final ObjectMapper protocolMapper = new ObjectMapper();
  /** Queries read state stores and Kafka topics; a few at a time, so the console can't take over the application. */
  private final Scheduler scheduler = Schedulers.newBoundedElastic(4, 100, "eventify-console");

  private volatile boolean running;
  private volatile Disposable connecting;
  private volatile RSocket connection;

  /**
   * @param consoleUrl the console's address, as opened in the browser, e.g. {@code http://eventify-console:8080}
   * @param nodeInfo   what this instance tells the console about itself
   * @param handler    answers a request: route name and request data in, reply out
   */
  public ConsoleConnector(URI consoleUrl, NodeInfo nodeInfo, BiFunction<String, byte[], ConsoleRequestHandler.Reply> handler) {
    this.uri = rsocketUri(consoleUrl);
    this.nodeInfo = nodeInfo;
    this.handler = handler;
  }

  public void start() {
    running = true;
    connect(Duration.ZERO);
  }

  public void stop() {
    running = false;
    Disposable pending = connecting;
    if (pending != null) {
      pending.dispose();
    }
    RSocket current = connection;
    if (current != null) {
      current.dispose();
    }
    scheduler.dispose();
  }

  /** Whether the connection to the console is open right now. */
  public boolean isConnected() {
    RSocket current = connection;
    return current != null && !current.isDisposed();
  }

  private void connect(Duration delay) {
    Mono<RSocket> connect = RSocketConnector.create()
        .setupPayload(Mono.fromCallable(() -> DefaultPayload.create(protocolMapper.writeValueAsBytes(nodeInfo))))
        .dataMimeType(ConsoleProtocol.DATA_MIME_TYPE)
        .metadataMimeType(ConsoleProtocol.METADATA_MIME_TYPE)
        .keepAlive(KEEPALIVE_INTERVAL, KEEPALIVE_MAX_LIFETIME)
        .fragment(FRAGMENT_SIZE)
        .acceptor(SocketAcceptor.forRequestResponse(this::handle))
        .connect(WebsocketClientTransport.create(uri))
        .retryWhen(Retry.backoff(Long.MAX_VALUE, MIN_BACKOFF)
            .maxBackoff(MAX_BACKOFF)
            .filter(e -> running)
            .doBeforeRetry(signal -> {
              // Once, not on every attempt: the console may well be down for a while.
              if (signal.totalRetries() == 0) {
                log.warn("Can't connect to the Eventify Console at {} ({}). Retrying in the background.", uri, signal.failure().toString());
              } else {
                log.debug("Can't connect to the Eventify Console at {} ({})", uri, signal.failure().toString());
              }
            }));

    connecting = Mono.delay(delay)
        .then(connect)
        .subscribe(this::onConnected, e -> {
          if (running) {
            log.error("Stopped connecting to the Eventify Console at {}", uri, e);
          }
        });
  }

  private void onConnected(RSocket rsocket) {
    if (!running) {
      rsocket.dispose();
      return;
    }
    connection = rsocket;
    log.info("Connected to the Eventify Console at {} as {}", uri, nodeInfo.nodeId());

    rsocket.onClose()
        .doFinally(signal -> {
          if (running) {
            log.warn("Lost the connection to the Eventify Console at {}. Reconnecting.", uri);
            connect(MIN_BACKOFF);
          }
        })
        .subscribe(null, e -> log.debug("Connection to the Eventify Console closed with an error", e));
  }

  private Mono<Payload> handle(Payload request) {
    String route;
    byte[] data;
    try {
      route = request.getMetadataUtf8();
      data = toBytes(request.getData());
    } finally {
      request.release();
    }

    return Mono.fromCallable(() -> toPayload(handler.apply(route, data)))
        .subscribeOn(scheduler)
        .onErrorResume(e -> {
          log.warn("Failed to handle console request for route {}", route, e);
          return Mono.fromCallable(() -> toPayload(ConsoleRequestHandler.Reply.of(ReplyHeader.unavailable("Too busy or unexpected error"))));
        });
  }

  private Payload toPayload(ConsoleRequestHandler.Reply reply) throws Exception {
    return DefaultPayload.create(reply.body(), protocolMapper.writeValueAsBytes(reply.header()));
  }

  private static byte[] toBytes(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }

  /** The console's address as its RSocket endpoint: {@code http://host:8080} becomes {@code ws://host:8080/rsocket}. */
  static URI rsocketUri(URI consoleUrl) {
    String scheme = switch (String.valueOf(consoleUrl.getScheme()).toLowerCase()) {
      case "http", "ws" -> "ws";
      case "https", "wss" -> "wss";
      default -> throw new IllegalArgumentException("The Eventify Console url must start with http:// or https://, but is " + consoleUrl);
    };
    String path = consoleUrl.getPath() == null ? "" : consoleUrl.getPath().replaceAll("/+$", "");
    if (!path.endsWith(ConsoleProtocol.RSOCKET_PATH)) {
      path = path + ConsoleProtocol.RSOCKET_PATH;
    }
    return URI.create(scheme + "://" + consoleUrl.getRawAuthority() + path);
  }
}
