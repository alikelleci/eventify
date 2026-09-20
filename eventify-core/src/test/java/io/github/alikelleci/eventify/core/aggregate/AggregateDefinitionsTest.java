package io.github.alikelleci.eventify.core.aggregate;

import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.EnableSnapshotting;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Aggregate definitions")
class AggregateDefinitionsTest {

  private final AggregateDefinitions definitions = new AggregateDefinitions(List.of(Order.class, Invoice.class));

  @AggregateRoot("order")
  record Order() {
  }

  @AggregateRoot("invoice")
  @EnableSnapshotting(threshold = 10, deleteEvents = true)
  record Invoice() {
  }

  @Test
  @DisplayName("Should expose the snapshot policy of an aggregate type")
  void exposesSnapshotPolicy() {
    assertThat(definitions.isSnapshotDue("order", 0, 100)).isFalse();
    assertThat(definitions.isSnapshotDue("invoice", 0, 10)).isTrue();
    assertThat(definitions.deletesEventsAtSnapshot("invoice")).isTrue();
  }

  @Test
  @DisplayName("Should reject an aggregate type this instance does not handle")
  void rejectsUnknownAggregateType() {
    assertThatThrownBy(() -> definitions.requireType("odrer"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("odrer");
  }
}
