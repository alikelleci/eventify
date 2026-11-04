package io.github.alikelleci.eventify.core.command.internal;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

/** The {@link HeaderNames#DELIVER_AT} header: written and read only here, in epoch milliseconds. */
public final class DeliverAt {

  private DeliverAt() {
  }

  /** The header for this delivery time. */
  public static Header header(Instant deliverAt) {
    return new RecordHeader(HeaderNames.DELIVER_AT, Long.toString(deliverAt.toEpochMilli()).getBytes(StandardCharsets.UTF_8));
  }

  /** The delivery time in epoch milliseconds; {@code null} when the header is missing or invalid. */
  public static Long millis(Headers headers) {
    Header header = headers != null ? headers.lastHeader(HeaderNames.DELIVER_AT) : null;
    if (header == null || header.value() == null) {
      return null;
    }
    try {
      long millis = Long.parseLong(new String(header.value(), StandardCharsets.UTF_8));
      return millis >= 0 ? millis : null;
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
