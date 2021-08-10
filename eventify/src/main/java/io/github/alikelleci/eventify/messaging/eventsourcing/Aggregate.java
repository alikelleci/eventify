package io.github.alikelleci.eventify.messaging.eventsourcing;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.github.alikelleci.eventify.common.annotations.EnableSnapshots;
import io.github.alikelleci.eventify.messaging.Message;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.springframework.core.annotation.AnnotationUtils;

import java.util.Optional;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString(callSuper = true)
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
public class Aggregate extends Message {
  private String aggregateId;
  private String eventId;
  private long version;


  @JsonIgnore
  public int getSnapshotTreshold() {
    return Optional.ofNullable(AnnotationUtils.findAnnotation(getPayload().getClass(), EnableSnapshots.class))
        .map(EnableSnapshots::threshold)
        .filter(threshold -> threshold > 0)
        .orElse(0);
  }
}
