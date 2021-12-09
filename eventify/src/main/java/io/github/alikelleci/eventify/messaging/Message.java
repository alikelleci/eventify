package io.github.alikelleci.eventify.messaging;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder(toBuilder = true)
public class Message {
  private String id;
  private Instant timestamp;
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
  private Object payload;
  private Metadata metadata;

  public Metadata getMetadata() {
    if (metadata == null) {
      return Metadata.builder()
          .messageId(id)
          .timestamp(timestamp)
          .build();
    }
    return metadata.toBuilder()
        .messageId(id)
        .timestamp(timestamp)
        .build();
  }
}
