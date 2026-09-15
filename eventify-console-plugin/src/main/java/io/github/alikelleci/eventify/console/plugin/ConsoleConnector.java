package io.github.alikelleci.eventify.console.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.console.protocol.ConsoleProtocol;
import io.github.alikelleci.eventify.console.protocol.NodeInfo;
import io.github.alikelleci.eventify.console.protocol.ReplyHeader;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.exceptions.RejectedSetupException;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.util.DefaultPayload;
import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

  /**
   * RSocket doesn't confirm that the console accepted a connection: a rejection (e.g. a wrong token)
   * arrives as the connection closing right after it opened. A connection still open after this long was accepted.
   */
  private static final Duration ACCEPTED_AFTER = Duration.ofSeconds(1);

  /** WebSocket frames are limited to 64 KB, so larger replies (a page of events) are sent in parts. */
  static final int FRAGMENT_SIZE = 16 * 1024;

  private final URI uri;
  private final String token;
  private final NodeInfo nodeInfo;
  private final Handler handler;
  private final ObjectMapper protocolMapper = new ObjectMapper();
  private final ThreadPoolExecutor executor = queryExecutor();

  private volatile boolean running;
  private volatile Disposable connecting;
  private volatile RSocket connection;
  private volatile boolean accepted;
  /** The last reason the console gave for rejecting this instance, so it's logged once, not on every attempt. */
  private volatile String lastRejection;

  /**
   * @param consoleUrl the console's address, as opened in the browser, e.g. {@code http://eventify-console:8080}
   * @param token      the console's application token, or {@code null} when the console doesn't require one
   * @param nodeInfo   what this instance tells the console about itself
   * @param handler    answers a request
   */
  public ConsoleConnector(URI consoleUrl, String token, NodeInfo nodeInfo, Handler handler) {
    this.uri = rsocketUri(consoleUrl);
    this.token = token;
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
    // Without interrupting queries that are still running: Kafka clients don't handle interrupts well.
    executor.shutdown();
  }

  /** Whether the console accepted this instance and the connection is open right now. */
  public boolean isConnected() {
    RSocket current = connection;
    return accepted && current != null && !current.isDisposed();
  }

  private void connect(Duration delay) {
    Mono<RSocket> connect = RSocketConnector.create()
        .setupPayload(Mono.fromCallable(() -> DefaultPayload.create(
            protocolMapper.writeValueAsBytes(nodeInfo),
            token == null ? new byte[0] : token.getBytes(StandardCharsets.UTF_8))))
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
    accepted = false;

    Mono.delay(ACCEPTED_AFTER)
        .takeUntilOther(rsocket.onClose().onErrorResume(e -> Mono.empty()))
        .subscribe(ignored -> {
          accepted = true;
          lastRejection = null;
          log.info("Connected to the Eventify Console at {} as {}", uri, nodeInfo.nodeId());
        });

    rsocket.onClose().subscribe(null, this::onClosed, () -> onClosed(null));
  }

  private void onClosed(Throwable error) {
    accepted = false;
    if (!running) {
      return;
    }

    if (error instanceof RejectedSetupException rejected) {
      // Trying again every second won't help (the console will refuse again), so try again slowly and say why once.
      if (!String.valueOf(rejected.getMessage()).equals(lastRejection)) {
        log.error("The Eventify Console at {} rejected this instance: {}. Trying again every {} seconds.",
            uri, rejected.getMessage(), MAX_BACKOFF.toSeconds());
      }
      lastRejection = String.valueOf(rejected.getMessage());
      connect(MAX_BACKOFF);
    } else {
      log.warn("Lost the connection to the Eventify Console at {}. Reconnecting.", uri);
      connect(MIN_BACKOFF);
    }
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

    // When the console cancels a request (someone refreshed the page, or the console stopped), the query gets the
    // signal: a query that hasn't started is skipped, a running one can stop cleanly (see EventifyService.getCommands).
    // The thread is never interrupted: that would break a Kafka consumer halfway a poll or close.
    CancelSignal cancel = new CancelSignal();
    return Mono.defer(() -> Mono.fromFuture(CompletableFuture.supplyAsync(() -> {
          if (cancel.isCancelled()) {
            return null;
          }
          try {
            return toPayload(handler.handle(route, data, cancel));
          } catch (Exception e) {
            throw new CompletionException(e);
          }
        }, executor), true))
        .doOnCancel(cancel::cancel)
        .onErrorResume(e -> {
          log.warn("Failed to handle console request for route {}", route, e);
          return Mono.fromCallable(() -> toPayload(ConsoleRequestHandler.Reply.of(ReplyHeader.unavailable("Too busy or unexpected error"))));
        });
  }

  /** Answers the console's requests. */
  @FunctionalInterface
  public interface Handler {
    /**
     * @param route  the route name
     * @param data   the request data
     * @param cancel tells when the console no longer waits for the answer
     */
    ConsoleRequestHandler.Reply handle(String route, byte[] data, CancelSignal cancel);
  }

  /** Queries read state stores and Kafka topics; a few at a time, so the console can't take over the application. */
  private static ThreadPoolExecutor queryExecutor() {
    AtomicInteger threads = new AtomicInteger();
    ThreadPoolExecutor executor = new ThreadPoolExecutor(4, 4, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), runnable -> {
      Thread thread = new Thread(runnable, "eventify-console-" + threads.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    });
    executor.allowCoreThreadTimeOut(true);
    return executor;
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
