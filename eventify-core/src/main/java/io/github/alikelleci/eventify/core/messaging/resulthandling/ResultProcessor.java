package io.github.alikelleci.eventify.core.messaging.resulthandling;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.messaging.Metadata;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.messaging.resulthandling.annotations.HandleFailure;
import io.github.alikelleci.eventify.core.messaging.resulthandling.annotations.HandleResult;
import io.github.alikelleci.eventify.core.messaging.resulthandling.annotations.HandleSuccess;
import java.util.Collection;
import java.util.Comparator;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public interface ResultProcessor {

  Eventify getEventify();

  default void processCommand(Command command) {
    Collection<ResultHandler> resultHandlers = getEventify().getResultHandlers().get(command.getPayload().getClass());
    if (CollectionUtils.isEmpty(resultHandlers)) {
      return;
    }

    resultHandlers.stream()
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
  }
}
