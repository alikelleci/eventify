package io.github.alikelleci.eventify.messaging.commandhandling;

import io.github.alikelleci.eventify.Eventify;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandResult.Success;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.eventify.messaging.eventsourcing.Repository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Optional;


@Slf4j
public class CommandTransformer implements ValueTransformerWithKey<String, Command, CommandResult> {

  private final Eventify.Builder builder;

  private Repository repository;

  public CommandTransformer(Eventify.Builder builder) {
    this.builder = builder;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.repository = new Repository(processorContext, builder);
  }

  @Override
  public CommandResult transform(String key, Command command) {
    CommandHandler commandHandler = builder.getCommandHandlers().get(command.getPayload().getClass());
    if (commandHandler == null) {
      return null;
    }

    // 1. Load aggregate state
    Aggregate aggregate = repository.loadAggregate(key);

    // 2. Validate command against aggregate
    CommandResult result = commandHandler.apply(command, aggregate);

    if (result instanceof Success) {
      // 3. Save events
      ((Success) result).getEvents().forEach(event ->
          repository.saveEvent(event));

      // 4. Save snapshot if needed
      Optional.ofNullable(aggregate)
          .filter(aggr -> aggr.getSnapshotTreshold() > 0)
          .filter(aggr -> aggr.getVersion() % aggr.getSnapshotTreshold() == 0)
          .ifPresent(aggr -> {
            log.debug("Creating snapshot: {}", aggr);
            repository.saveSnapshot(aggr);

            // 5. Delete events after snapshot
            if (builder.isDeleteEventsOnSnapshot()) {
              log.debug("Events prior to this snapshot will be deleted");
              repository.deleteEvents(aggr);
            }
          });
    }

    return result;
  }

  @Override
  public void close() {

  }
}
