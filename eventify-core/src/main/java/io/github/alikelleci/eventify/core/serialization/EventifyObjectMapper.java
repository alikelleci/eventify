package io.github.alikelleci.eventify.core.serialization;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/** The ObjectMapper Eventify uses when none is given: the one its messages are written and read with by default. */
public class EventifyObjectMapper {

  private static ObjectMapper objectMapper;

  private EventifyObjectMapper() {
  }

  public static ObjectMapper get() {
    if (objectMapper == null) {
      objectMapper = new ObjectMapper()
          .findAndRegisterModules()
          .setSerializationInclusion(JsonInclude.Include.NON_NULL)
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
          .configure(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE, false)
          .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
          .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
          .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    }
    return objectMapper;
  }
}
