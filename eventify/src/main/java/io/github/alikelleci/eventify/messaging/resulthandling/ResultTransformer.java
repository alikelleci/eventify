package io.github.alikelleci.eventify.messaging.resulthandling;

import io.github.alikelleci.eventify.constants.Handlers;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.messaging.resulthandling.annotations.HandleFailure;
import io.github.alikelleci.eventify.messaging.resulthandling.annotations.HandleResult;
import io.github.alikelleci.eventify.messaging.resulthandling.annotations.HandleSuccess;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;

@Slf4j
public class ResultTransformer implements ValueTransformerWithKey<String, Command, Command> {

  private ProcessorContext context;

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public Command transform(String key, Command command) {
    Collection<ResultHandler> handlers = Handlers.RESULT_HANDLERS.get(command.getPayload().getClass());
    if (CollectionUtils.isEmpty(handlers)) {
      return null;
    }

    handlers.stream()
        .sorted(Comparator.comparingInt(ResultHandler::getPriority).reversed())
        .forEach(handler -> {
          boolean handleAll = handler.getMethod().isAnnotationPresent(HandleResult.class);
          boolean handleSuccess = handler.getMethod().isAnnotationPresent(HandleSuccess.class);
          boolean handleFailure = handler.getMethod().isAnnotationPresent(HandleFailure.class);

          String result = command.getMetadata().get(Metadata.RESULT);
          if (handleAll ||
              (handleSuccess && StringUtils.equals(result, "success")) ||
              (handleFailure && StringUtils.equals(result, "failure"))) {
            handler.apply(command);
          }
        });

    return command;
  }

  @Override
  public void close() {

  }


}
