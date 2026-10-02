package io.github.alikelleci.eventify.core.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Eventify's ObjectMapper")
class EventifyObjectMapperTest {

  @Test
  @DisplayName("Should give a copy each time: changing one doesn't change how Eventify writes events")
  void aChangedCopyDoesNotChangeTheOthers() {
    ObjectMapper changed = EventifyObjectMapper.create();
    changed.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);

    ObjectMapper next = EventifyObjectMapper.create();

    assertThat(next).isNotSameAs(changed);
    assertThat(next.isEnabled(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)).isFalse();
  }
}
