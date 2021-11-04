package io.github.alikelleci.eventify.messaging;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder(toBuilder = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public class Message {
  private String messageId;
  private long timestamp;
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
  private Object payload;
  private Metadata metadata;

  public Metadata getMetadata() {
    if (metadata == null) {
      return Metadata.builder()
          .messageId(messageId)
          .timestamp(timestamp)
          .build();
    }
    return metadata.toBuilder()
        .messageId(messageId)
        .timestamp(timestamp)
        .build();
  }
}
