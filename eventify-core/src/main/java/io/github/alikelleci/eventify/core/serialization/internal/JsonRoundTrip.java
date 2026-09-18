package io.github.alikelleci.eventify.core.serialization.internal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/** A copy of an object made by writing it as JSON and reading it back: as it will be when it is stored or sent. */
public final class JsonRoundTrip {

  private JsonRoundTrip() {
  }

  /**
   * @param name what the object is, for the error, e.g. "Event OrderPlaced"
   * @throws IllegalArgumentException when the object can't be written as JSON, or read back
   */
  public static <T> T copy(ObjectMapper objectMapper, T value, Class<T> type, String name) {
    byte[] json;
    try {
      json = objectMapper.writeValueAsBytes(value);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(name + " cannot be written as JSON: " + e.getOriginalMessage(), e);
    }
    try {
      return objectMapper.readValue(json, type);
    } catch (IOException e) {
      throw new IllegalArgumentException(name + " cannot be read back from JSON: " + e.getMessage(), e);
    }
  }
}
