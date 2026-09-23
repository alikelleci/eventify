package io.github.alikelleci.eventify.core.serialization;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/** Eventify's default ObjectMapper; a new copy per call, so nobody can change the shared configuration. */
public final class EventifyObjectMapper {

  private EventifyObjectMapper() {
  }

  /** A new ObjectMapper with Eventify's configuration. */
  public static ObjectMapper create() {
    return Configured.MAPPER.copy();
  }

  /** Lazy, thread-safe holder. */
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
