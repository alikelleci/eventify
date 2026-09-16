package io.github.alikelleci.eventify.console.server.api;

import io.github.alikelleci.eventify.console.server.node.ConnectedNode;
import io.github.alikelleci.eventify.console.server.node.NodeRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/** The applications connected to the console right now, with their instances and how each one is doing. */
@RestController
@RequestMapping("/api/apps")
@RequiredArgsConstructor
public class ApplicationsController {

  private final NodeRegistry registry;
  private final InstanceStatuses statuses;

  @GetMapping
  public Mono<List<ApplicationView>> applications() {
    return Flux.fromIterable(registry.nodes().stream().collect(Collectors.groupingBy(ConnectedNode::applicationId)).entrySet())
        .flatMap(entry -> Flux.fromIterable(entry.getValue())
            .flatMap(node -> statuses.of(node).map(status ->
                new ApplicationView.NodeView(node.nodeId(), node.info().hostname(), node.info().version(), node.connectedAt(), status.orElse(null))))
            .collectSortedList(Comparator.comparing(ApplicationView.NodeView::nodeId))
            .map(nodes -> new ApplicationView(entry.getKey(), nodes)))
        .collectSortedList(Comparator.comparing(ApplicationView::name));
  }
}
