package io.github.alikelleci.eventify.messaging.commandhandling;

import io.github.alikelleci.eventify.constants.Handlers;
import io.github.alikelleci.eventify.constants.Topics;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.support.serializer.CustomSerdes;
import io.github.alikelleci.eventify.util.CommonUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class CommandBus {

  public void subscribe() {
    StreamsBuilder builder = new StreamsBuilder();

    /*
    *************************************************************************************
    * Command Handling
    *************************************************************************************
    */

    if (!Handlers.COMMAND_HANDLERS.isEmpty()) {

      // --> Commands
      KStream<String, Command> commands = builder.stream(Topics.COMMANDS, Consumed.with(Serdes.String(), CustomSerdes.Json(Command.class)))
          .filter((key, command) -> key != null)
          .filter((key, command) -> command != null);

      // Commands --> Results
      KStream<String, CommandResult> commandResults = commands
          .transformValues(CommandTransformer::new, "event-store", "snapshot-store")
          .filter((key, result) -> result != null);

      // Results --> Push
      commandResults
          .mapValues(CommandResult::getCommand)
          .to((key, command, recordContext) -> CommonUtils.getTopicInfo(command.getPayload()).value().concat(".results"),
              Produced.with(Serdes.String(), CustomSerdes.Json(Command.class)));

      // Events --> Push
      commandResults
          .filter((key, result) -> result instanceof CommandResult.Success)
          .mapValues((key, result) -> (CommandResult.Success) result)
          .flatMapValues(CommandResult.Success::getEvents)
          .filter((key, event) -> event != null)
          .to((key, event, recordContext) -> CommonUtils.getTopicInfo(event.getPayload()).value(),
              Produced.with(Serdes.String(), CustomSerdes.Json(Event.class)));

    }


    /*
     *************************************************************************************
     * Event Handling
     *************************************************************************************
     */

    if (!Handlers.COMMAND_HANDLERS.isEmpty()) {
      // --> Events
      KStream<String, Command> events = builder.stream(Topics.EVENTS, Consumed.with(Serdes.String(), CustomSerdes.Json(Command.class)))
          .filter((key, command) -> key != null)
          .filter((key, command) -> command != null);
    }


    /*
     *************************************************************************************
     * Result Handling
     *************************************************************************************
     */

    // --> Results
    KStream<String, Command> results = builder.stream(Topics.RESULTS, Consumed.with(Serdes.String(), CustomSerdes.Json(Command.class)))
        .filter((key, command) -> key != null)
        .filter((key, command) -> command != null);
  }


  public void dispatch(Command command) {

  }
}
