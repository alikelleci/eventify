package io.github.alikelleci.eventify.messaging.eventhandling;

import io.github.alikelleci.eventify.messaging.Message;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString(callSuper = true)
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
public class Event extends Message {
  private String aggregateId;
  private String aggregateType;
  private String type;
}
