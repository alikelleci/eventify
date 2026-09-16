package io.github.alikelleci.eventify.console.server.api;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.alikelleci.eventify.console.protocol.InstanceStatus;
import io.github.alikelleci.eventify.console.protocol.ReplyHeader;
import io.github.alikelleci.eventify.console.protocol.Route;
import io.github.alikelleci.eventify.console.server.node.ConnectedNode;
import io.github.alikelleci.eventify.console.server.node.NodeGateway;
import io.github.alikelleci.eventify.console.server.node.Reply;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;

/**
 * How the connected applications are doing. Every instance is asked for its own status; the answers are combined into
 * one status per application. The UI asks for this every few seconds, from every open page, so the answers are kept
 * for a moment: pages asking at the same time share one round of questions to the instances.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ApplicationStatuses {

  /** Which state to show when the instances of an application are in different ones: the one worst off wins. */
  private static final List<String> SEVERITY = List.of(
      "ERROR", "PENDING_ERROR", "NOT_RUNNING", "PENDING_SHUTDOWN", "REBALANCING", "CREATED", "RUNNING");

  /** How long the answers are kept. Shorter than the UI asks, so every page still sees a fresh status each time. */
  private static final Duration KEEP = Duration.ofSeconds(2);

  /** How long to wait for an instance. The list of applications waits for this, so an instance that hangs is left out. */
  private static final Duration TIMEOUT = Duration.ofSeconds(2);

  private final NodeGateway gateway;
  private final JsonMapper jsonMapper;

  private final AsyncCache<String, ApplicationStatusView> cache = Caffeine.newBuilder()
      .expireAfterWrite(KEEP)
      .buildAsync();

  /** The status of one application, asked of the given instances unless it was asked a moment ago. */
  public Mono<ApplicationStatusView> of(String application, List<ConnectedNode> nodes) {
    return Mono.fromFuture(() -> cache.get(application, (key, executor) -> ask(application, nodes).toFuture()));
  }

  /** Asks all instances of one application at the same time; an instance that doesn't answer is left out. */
  private Mono<ApplicationStatusView> ask(String application, List<ConnectedNode> nodes) {
    return Flux.fromIterable(nodes)
        .flatMap(node -> gateway.sendTo(node, Route.STATUS, new byte[0])
            .timeout(TIMEOUT, Mono.empty())
            .mapNotNull(this::read))
        .collectList()
        .map(ApplicationStatuses::combine);
  }

  private InstanceStatus read(Reply reply) {
    if (reply.header().status() != ReplyHeader.Status.OK || reply.body().length == 0) {
      return null;
    }
    try {
      InstanceStatus status = jsonMapper.readValue(reply.body(), InstanceStatus.class);
      return status.state() != null ? status : null;
    } catch (Exception e) {
      log.warn("Could not read the status of an instance", e);
      return null;
    }
  }

  private static ApplicationStatusView combine(List<InstanceStatus> answers) {
    if (answers.isEmpty()) {
      return new ApplicationStatusView(null, 0, 0, null, 0);
    }

    String state = answers.stream()
        .map(InstanceStatus::state)
        .min(Comparator.comparingInt(ApplicationStatuses::severity))
        .orElse(null);
    // The application has been in that state since the first instance entered it: the longest of them.
    long stateForMs = answers.stream()
        .filter(answer -> answer.state().equals(state))
        .mapToLong(InstanceStatus::stateForMs)
        .max().orElse(0);
    int inState = (int) answers.stream().filter(answer -> answer.state().equals(state)).count();

    List<InstanceStatus.Restore> restores = answers.stream().map(InstanceStatus::restore).filter(restore -> restore != null).toList();
    long restored = restores.stream().mapToLong(InstanceStatus.Restore::restored).sum();
    long total = restores.stream().mapToLong(InstanceStatus.Restore::total).sum();
    ApplicationStatusView.Restore restore = total > 0
        ? new ApplicationStatusView.Restore(restored, total, (int) (restored * 100 / total), restores.size())
        : null;

    return new ApplicationStatusView(state, stateForMs, inState, restore, answers.size());
  }

  /** Lower is worse off; an unknown state is treated as the worst, so it can't hide behind a running one. */
  private static int severity(String state) {
    int index = SEVERITY.indexOf(state);
    return index < 0 ? -1 : index;
  }
}
