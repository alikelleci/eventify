package io.github.alikelleci.eventify.messaging.commandhandling;

import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandResult.Failure;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandResult.Success;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;
import org.apache.commons.lang3.EnumUtils;

import static io.github.alikelleci.eventify.messaging.Metadata.ID;
import static io.github.alikelleci.eventify.messaging.Metadata.REPLY_MODE;

@Value
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class CommandResponse extends Message {

  String aggregateId;

  private CommandResponse() {
    this.aggregateId = null;
  }

  @Builder
  private CommandResponse(CommandResult commandResult) {
    super(commandResult.getCommand().getTimestamp(), determinePayloadType(commandResult), commandResult.getCommand().getMetadata());
    this.aggregateId = commandResult.getCommand().getAggregateId();
    this.id = this.aggregateId + "@" + getId();

    this.metadata.add(ID, getId());
  }

  private static Object determinePayloadType(CommandResult result) {
    ReplyMode replyMode = EnumUtils.getEnumIgnoreCase(ReplyMode.class, result.getCommand().getMetadata().get(REPLY_MODE));
    if (replyMode == null) {
      return result.getCommand();
    }

    if (result instanceof Failure failure) {
      return failure.getCommand();
    }

    if (result instanceof Success success) {
      return switch (replyMode) {
        case COMMAND -> success.getCommand();
        case EVENT -> success.getEvents();
      };
    }

    return result.getCommand();
  }

  public enum ReplyMode {
    COMMAND,
    EVENT
  }
}
