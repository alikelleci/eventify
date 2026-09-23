package io.github.alikelleci.eventify.core.kafka.internal;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.errors.LogAndContinueProcessingExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailProcessingExceptionHandler;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Streams config defaults")
class StreamsConfigDefaultsTest {

  @Test
  @DisplayName("Should always fail on processing and production errors, like exactly-once")
  void failsOnErrorsWhateverIsConfigured() {
    Properties config = new Properties();
    config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
    config.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueProcessingExceptionHandler.class);
    config.put(StreamsConfig.PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, "com.example.ContinueHandler");

    StreamsConfigDefaults.apply(config);

    assertThat(config.get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)).isEqualTo(StreamsConfig.EXACTLY_ONCE_V2);
    assertThat(config.get(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG)).isEqualTo(LogAndFailProcessingExceptionHandler.class.getName());
    assertThat(config.get(StreamsConfig.PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG)).isEqualTo(DefaultProductionExceptionHandler.class.getName());
  }
}
