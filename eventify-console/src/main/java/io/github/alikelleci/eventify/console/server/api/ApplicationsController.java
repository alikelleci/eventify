package io.github.alikelleci.eventify.console.server.api;

import io.github.alikelleci.eventify.console.server.node.ConnectedNode;
import io.github.alikelleci.eventify.console.server.node.NodeRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/** The applications connected to the console right now, with their instances. How they are doing is {@link StatusController}. */
@RestController
@RequestMapping("/api/apps")
@RequiredArgsConstructor
public class ApplicationsController {

  private final NodeRegistry registry;

  @GetMapping
  public List<ApplicationView> applications() {
    return registry.nodes().stream()
        .collect(Collectors.groupingBy(ConnectedNode::applicationId))
        .entrySet().stream()
        .map(entry -> new ApplicationView(entry.getKey(), entry.getValue().stream()
            .sorted(Comparator.comparing(ConnectedNode::nodeId))
            .map(node -> new ApplicationView.NodeView(node.nodeId(), node.info().hostname(), node.info().version(), node.connectedAt()))
            .toList()))
        .sorted(Comparator.comparing(ApplicationView::name))
        .toList();
  }
}
