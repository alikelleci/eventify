package io.github.alikelleci.eventify.core;

import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandResult;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.message.MetadataKeys;
import io.github.alikelleci.eventify.core.serialization.JsonDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.github.alikelleci.eventify.core.support.CommandFactory.buildPlaceOrderCommand;
import static org.assertj.core.api.Assertions.assertThat;

/** The result of a command, as its sender and the result topic get it. */
@DisplayName("Command result")
class CommandResultTest {

  private static final String REPLY_TOPIC = "orders-api.replies";

  private TopologyTestDriver driver;
  private TestInputTopic<String, Command> commands;
  private TestOutputTopic<String, CommandResult> results;
  private TestOutputTopic<String, CommandResult> replies;
  private TestOutputTopic<String, Event> events;

  @BeforeEach
  void setUp() {
    driver = new TopologyTestDriver(EventifyTest.baseBuilder().build().topology());
    commands = EventifyTest.commandsTopic(driver);
    results = EventifyTest.commandResultsTopic(driver);
    replies = driver.createOutputTopic(REPLY_TOPIC, new StringDeserializer(), new JsonDeserializer<>(CommandResult.class));
    events = EventifyTest.eventsTopic(driver);
  }

  @AfterEach
  void tearDown() {
    driver.close();
  }

  @Test
  @DisplayName("Should give the sender the events of the command, as they are stored and sent")
  void theSenderGetsTheEvents() {
    Command command = buildPlaceOrderCommand("order-1");
    command.getMetadata().put(MetadataKeys.REPLY_TO, REPLY_TOPIC);

    commands.pipeInput(command.getAggregateId(), command);

    List<Event> sent = events.readValuesToList();
    CommandResult reply = replies.readValue();
    assertThat(sent).hasSize(1);
    assertThat(reply).isInstanceOfSatisfying(CommandResult.Success.class, success -> {
      assertThat(success.command().getId()).isEqualTo(command.getId());
      assertThat(success.events()).extracting(Event::getId).containsExactly(sent.get(0).getId());
      assertThat(success.events().get(0).getPayload()).isEqualTo(sent.get(0).getPayload());
    });
    assertThat(results.readValue()).isEqualTo(reply);
  }

  @Test
  @DisplayName("Should not send a result to a reply topic when the sender doesn't wait for it")
  void noReplyWithoutAReplyTopic() {
    Command command = buildPlaceOrderCommand("order-1");
    command.getMetadata().remove(MetadataKeys.REPLY_TO);

    commands.pipeInput(command.getAggregateId(), command);

    assertThat(results.readValue()).isInstanceOf(CommandResult.Success.class);
    assertThat(replies.isEmpty()).isTrue();
  }
}
