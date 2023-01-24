package io.github.alikelleci.eventify.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class JacksonUtils {

  private static ObjectMapper objectMapper;

  private JacksonUtils() {
  }

  public static ObjectMapper enhancedObjectMapper() {
    if (objectMapper == null) {
      objectMapper = new ObjectMapper()
          .findAndRegisterModules()
          .setSerializationInclusion(JsonInclude.Include.NON_NULL)
//          .configure(MapperFeature.DEFAULT_VIEW_INCLUSION, false)
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
          .configure(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE, false)
//          .configure(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
//          .configure(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
//          .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true)
          .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }
    return objectMapper;
  }
}
