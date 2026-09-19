package io.github.alikelleci.eventify.core.serialization;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * The ObjectMapper Eventify uses when none is given: the one its messages are written and read with by default.
 *
 * <p>Every call gives a new copy. A shared one could be changed by any code that got it, and would then change how
 * Eventify writes and reads its events, without a word.
 */
public final class EventifyObjectMapper {

  private EventifyObjectMapper() {
  }

  /** A new ObjectMapper with Eventify's configuration. */
  public static ObjectMapper create() {
    return Configured.MAPPER.copy();
  }

  /** Configured once, when first used: the JVM initializes a class once, safely for every thread. */
  private static final class Configured {
    static final ObjectMapper MAPPER = new ObjectMapper()
        .findAndRegisterModules()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE, false)
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
        .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
  }
}
