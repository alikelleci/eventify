package io.github.alikelleci.eventify.core.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.serialization.EventifyObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Schedule token")
class ScheduleTokenTest {

  /** It is kept, for example in an aggregate's state, to cancel with later. */
  @Test
  @DisplayName("Should read back as written as JSON")
  void readsBackAsWritten() throws Exception {
    ObjectMapper objectMapper = EventifyObjectMapper.create();
    ScheduleToken token = new ScheduleToken("orders", "order-1", "c-1", Instant.parse("2030-01-01T00:00:00.123Z"));

    assertThat(objectMapper.readValue(objectMapper.writeValueAsBytes(token), ScheduleToken.class)).isEqualTo(token);
  }

  @Test
  @DisplayName("Should refuse a missing part")
  void refusesAMissingPart() {
    assertThatThrownBy(() -> new ScheduleToken(null, "order-1", "c-1", Instant.now())).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new ScheduleToken("orders", null, "c-1", Instant.now())).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new ScheduleToken("orders", "order-1", null, Instant.now())).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new ScheduleToken("orders", "order-1", "c-1", null)).isInstanceOf(NullPointerException.class);
  }
}
