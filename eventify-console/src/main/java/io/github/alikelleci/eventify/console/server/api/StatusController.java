package io.github.alikelleci.eventify.console.server.api;

import io.github.alikelleci.eventify.console.protocol.InstanceStatus;
import io.github.alikelleci.eventify.console.protocol.ReplyHeader;
import io.github.alikelleci.eventify.console.protocol.Route;
import io.github.alikelleci.eventify.console.server.node.ConnectedNode;
import io.github.alikelleci.eventify.console.server.node.NodeGateway;
import io.github.alikelleci.eventify.console.server.node.NodeRegistry;
import io.github.alikelleci.eventify.console.server.node.Reply;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * How the connected applications are doing. Every instance is asked for its own status; the answers are combined into
 * one line per application. The page asks for this every few seconds while it is open, so it is kept cheap: the
 * instances only read what they already have in memory.
 */
@Slf4j
@RestController
@RequestMapping("/api/status")
@RequiredArgsConstructor
public class StatusController {

  /** Which state to show when the instances of an application are in different ones: the one worst off wins. */
  private static final List<String> SEVERITY = List.of(
      "ERROR", "PENDING_ERROR", "NOT_RUNNING", "PENDING_SHUTDOWN", "REBALANCING", "CREATED", "RUNNING");

  private final NodeRegistry registry;
  private final NodeGateway gateway;
  private final JsonMapper jsonMapper;

  @GetMapping
  public Mono<List<ApplicationStatusView>> status() {
    Map<String, List<ConnectedNode>> byApplication = registry.nodes().stream()
        .collect(Collectors.groupingBy(ConnectedNode::applicationId));

    return Flux.fromIterable(byApplication.entrySet())
        .flatMap(entry -> statusOf(entry.getKey(), entry.getValue()))
        .collectSortedList(Comparator.comparing(ApplicationStatusView::name));
  }

  /** Asks all instances of one application at the same time; an instance that doesn't answer is left out. */
  private Mono<ApplicationStatusView> statusOf(String application, List<ConnectedNode> nodes) {
    return Flux.fromIterable(nodes)
        .flatMap(node -> gateway.sendTo(node, Route.STATUS, new byte[0]).mapNotNull(this::read))
        .collectList()
        .map(answers -> combine(application, nodes.size(), answers));
  }

  private InstanceStatus read(Reply reply) {
    if (reply.header().status() != ReplyHeader.Status.OK || reply.body().length == 0) {
      return null;
    }
    try {
      return jsonMapper.readValue(reply.body(), InstanceStatus.class);
    } catch (Exception e) {
      log.warn("Could not read the status of an instance", e);
      return null;
    }
  }

  private static ApplicationStatusView combine(String application, int instances, List<InstanceStatus> answers) {
    if (answers.isEmpty()) {
      return new ApplicationStatusView(application, null, 0, 0, null, null, instances, 0);
    }

    String state = answers.stream()
        .map(InstanceStatus::state)
        .min(Comparator.comparingInt(StatusController::severity))
        .orElse(null);
    // The application has been in that state since the first instance entered it: the longest of them.
    long stateForMs = answers.stream()
        .filter(answer -> answer.state().equals(state))
        .mapToLong(InstanceStatus::stateForMs)
        .max().orElse(0);
    int inState = (int) answers.stream().filter(answer -> answer.state().equals(state)).count();

    Long commandsInQueue = answers.stream().anyMatch(answer -> answer.commandsInQueue() != null)
        ? answers.stream().filter(answer -> answer.commandsInQueue() != null).mapToLong(InstanceStatus::commandsInQueue).sum()
        : null;

    long restored = answers.stream().filter(answer -> answer.restore() != null).mapToLong(answer -> answer.restore().restored()).sum();
    long total = answers.stream().filter(answer -> answer.restore() != null).mapToLong(answer -> answer.restore().total()).sum();
    ApplicationStatusView.Restore restore = total > 0
        ? new ApplicationStatusView.Restore(restored, total, (int) (restored * 100 / total))
        : null;

    return new ApplicationStatusView(application, state, stateForMs, inState, commandsInQueue, restore, instances, answers.size());
  }

  /** Lower is worse off; an unknown state is treated as the worst, so it can't hide behind a running one. */
  private static int severity(String state) {
    int index = SEVERITY.indexOf(state);
    return index < 0 ? -1 : index;
  }
}
