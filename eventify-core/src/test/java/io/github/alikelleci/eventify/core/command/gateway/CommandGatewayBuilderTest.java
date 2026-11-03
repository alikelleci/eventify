package io.github.alikelleci.eventify.core.command.gateway;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Command gateway: builder")
class CommandGatewayBuilderTest {

  @Test
  @DisplayName("Should say producerConfig is missing")
  void withoutProducerConfig() {
    assertThatThrownBy(() -> CommandGateway.builder().replyTopic("replies").build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("producerConfig");
  }

  @Test
  @DisplayName("Should say replyTopic is missing")
  void withoutReplyTopic() {
    Properties producerConfig = new Properties();
    producerConfig.put("bootstrap.servers", "localhost:9092");

    assertThatThrownBy(() -> CommandGateway.builder().producerConfig(producerConfig).build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("replyTopic");
  }
}
