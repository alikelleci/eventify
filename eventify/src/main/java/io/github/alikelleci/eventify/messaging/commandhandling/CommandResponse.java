package io.github.alikelleci.eventify.messaging.commandhandling;

import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandResult.Failure;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandResult.Success;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Value;
import org.apache.commons.lang3.EnumUtils;

import static io.github.alikelleci.eventify.messaging.Metadata.CORRELATION_ID;
import static io.github.alikelleci.eventify.messaging.Metadata.REPLY_MODE;
import static io.github.alikelleci.eventify.messaging.Metadata.REPLY_TO;

@Getter
public class CommandResponse {
  String correlationId;
  String replyTopic;
  String message;
  boolean success;
  Object payload;

  @Builder
  protected CommandResponse(CommandResult commandResult) {
    Metadata metadata = commandResult.getCommand().getMetadata();

    this.correlationId = metadata.get(CORRELATION_ID);
    this.replyTopic = metadata.get(REPLY_TO);

    if (commandResult instanceof Success result) {
      this.message = "Accepted";
      this.success = true;
      this.payload = determinePayloadType(result, metadata);

    } else if (commandResult instanceof Failure result) {
      this.message = result.getCause();
      this.success = false;
    }
  }

  private Object determinePayloadType(Success result, Metadata metadata) {
    ReplyMode replyMode = EnumUtils.getEnumIgnoreCase(ReplyMode.class, metadata.get(REPLY_MODE));

    if (replyMode == null) {
      return null;
    }

    Object o;
    switch (replyMode) {
      case COMMAND -> o = result.getCommand();
      case EVENT -> o = result.getEvents();
      default -> o = null;
    }

    return o;
  }

  public enum ReplyMode {
    COMMAND,
    EVENT
  }

}
