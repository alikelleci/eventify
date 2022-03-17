package io.github.alikelleci.eventify.constants;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import java.util.Properties;

@Slf4j
public class Config {
  public static Properties streamsConfig = new Properties();

  public static KafkaStreams.StateListener stateListener = (newState, oldState) ->
      log.warn("State changed from {} to {}", oldState, newState);

  public static StreamsUncaughtExceptionHandler uncaughtExceptionHandler = (throwable) ->
      StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;

  public static boolean deleteEventsOnSnapshot = false;
}
