package io.github.alikelleci.eventify.console.server.node;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.alikelleci.eventify.console.protocol.Reply;
import io.github.alikelleci.eventify.console.protocol.ReplyHeader;
import io.github.alikelleci.eventify.console.protocol.RequestHeader;
import io.github.alikelleci.eventify.console.protocol.Route;
import io.github.alikelleci.eventify.console.server.ConsoleProperties;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;

/**
 * Sends a request to an instance of an application, to the one its {@link Route.Target} says.
 *
 * <p>Queries about an aggregate can only be answered by the instance that owns it. The console doesn't know which one
 * that is, but every instance does (Kafka Streams tells them): the first instance asked either answers, or replies
 * with the owner's name. The request then goes to the owner, once. The owner is remembered for next time.
 */
@Slf4j
@Component
public class NodeGateway {

  private final NodeRegistry registry;
  private final JsonMapper jsonMapper;
  private final Duration requestTimeout;

  /** The instance that last answered for an aggregate: asked first next time, which usually saves the redirect. */
  private final Cache<OwnerKey, String> owners = Caffeine.newBuilder()
      .maximumSize(10_000)
      .expireAfterAccess(Duration.ofMinutes(10))
      .build();

  private record OwnerKey(String applicationId, String aggregateId) {
  }

  public NodeGateway(NodeRegistry registry, JsonMapper jsonMapper, ConsoleProperties properties) {
    this.registry = registry;
    this.jsonMapper = jsonMapper;
    this.requestTimeout = properties.requestTimeout();
  }

  /** To the instance that owns the aggregate, for {@link Route.Target#OWNER} routes. */
  public Mono<Reply> sendToOwner(String applicationId, Route route, String aggregateId, byte[] data) {
    requireTarget(route, Route.Target.OWNER);
    OwnerKey key = new OwnerKey(applicationId, aggregateId);
    ConnectedNode first = nodeOf(applicationId, owners.getIfPresent(key));
    if (first == null) {
      first = registry.nextNode(applicationId);
    }
    if (first == null) {
      return Mono.just(notConnected(applicationId));
    }

    ConnectedNode asked = first;
    return request(asked, route, data).flatMap(reply -> {
      if (reply.header().status() != ReplyHeader.Status.NOT_OWNER) {
        remember(key, asked, reply);
        return Mono.just(reply);
      }

      ConnectedNode owner = nodeOf(applicationId, reply.header().owner());
      if (owner == null || owner == asked) {
        owners.invalidate(key);
        return Mono.just(Reply.of(ReplyHeader.unavailable(
            "The instance that owns this aggregate is not connected. The application may be rebalancing: try again.")));
      }

      return request(owner, route, data).map(second -> {
        if (second.header().status() == ReplyHeader.Status.NOT_OWNER) {
          // The instances don't agree on the owner: partitions are moving right now.
          owners.invalidate(key);
          return Reply.of(ReplyHeader.unavailable("The application is rebalancing: try again."));
        }
        remember(key, owner, second);
        return second;
      });
    });
  }

  private void remember(OwnerKey key, ConnectedNode node, Reply reply) {
    ReplyHeader.Status status = reply.header().status();
    if (status == ReplyHeader.Status.OK || status == ReplyHeader.Status.NOT_FOUND) {
      owners.put(key, node.nodeId());
    }
  }

  /** The connected instance with this name, if it belongs to the application. */
  private ConnectedNode nodeOf(String applicationId, String nodeId) {
    ConnectedNode node = registry.find(nodeId);
    return node != null && node.applicationId().equals(applicationId) ? node : null;
  }

  /** To one of the application's instances, a different one each time, for {@link Route.Target#ANY} routes. */
  public Mono<Reply> sendToAny(String applicationId, Route route, byte[] data) {
    requireTarget(route, Route.Target.ANY);
    ConnectedNode node = registry.nextNode(applicationId);
    return node == null ? Mono.just(notConnected(applicationId)) : request(node, route, data);
  }

  /** To this instance, for {@link Route.Target#INSTANCE} routes: requests about the instance itself. */
  public Mono<Reply> sendTo(ConnectedNode node, Route route, byte[] data) {
    requireTarget(route, Route.Target.INSTANCE);
    return request(node, route, data);
  }

  private static void requireTarget(Route route, Route.Target target) {
    if (route.target() != target) {
      throw new IllegalArgumentException("Route " + route + " goes to " + route.target() + ", not " + target);
    }
  }

  private Mono<Reply> request(ConnectedNode node, Route route, byte[] data) {
    Payload request = DefaultPayload.create(data, jsonMapper.writeValueAsBytes(RequestHeader.of(route)));
    return node.rsocket().requestResponse(request)
        .timeout(requestTimeout)
        .map(this::toReply)
        .switchIfEmpty(Mono.fromSupplier(() -> Reply.of(ReplyHeader.unavailable("Empty answer from instance " + node.nodeId()))))
        .onErrorResume(e -> {
          log.warn("Request {} to instance {} of application {} failed: {}", route, node.nodeId(), node.applicationId(), e.toString());
          return Mono.just(Reply.of(ReplyHeader.unavailable("No answer from instance " + node.nodeId())));
        });
  }

  private Reply toReply(Payload payload) {
    try {
      return new Reply(readHeader(payload.getMetadataUtf8()), toBytes(payload.getData()));
    } finally {
      payload.release();
    }
  }

  /**
   * The reply header. An instance on a newer version may answer with a status this console doesn't know: that is an
   * answer it can't use, so it counts as unavailable instead of failing the whole reply.
   */
  private ReplyHeader readHeader(String json) {
    JsonNode header = jsonMapper.readTree(json);
    String status = header.path("status").asString(null);
    if (Arrays.stream(ReplyHeader.Status.values()).noneMatch(known -> known.name().equals(status))) {
      log.warn("An instance answered with status '{}', which this console does not know", status);
      return ReplyHeader.unavailable("The application answered with something this console does not understand: upgrade the console");
    }
    return jsonMapper.treeToValue(header, ReplyHeader.class);
  }

  private static Reply notConnected(String applicationId) {
    return Reply.of(ReplyHeader.unavailable("No instance of application '" + applicationId + "' is connected"));
  }

  private static byte[] toBytes(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }
}
