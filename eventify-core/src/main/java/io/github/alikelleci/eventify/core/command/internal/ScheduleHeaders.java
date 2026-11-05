package io.github.alikelleci.eventify.core.command.internal;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

/** The headers of scheduled command and cancel records: written and read only here. */
public final class ScheduleHeaders {

  private ScheduleHeaders() {
  }

  /** The {@link HeaderNames#DELIVER_AT} header for this delivery time, in epoch milliseconds. */
  public static Header deliverAt(Instant deliverAt) {
    return new RecordHeader(HeaderNames.DELIVER_AT, Long.toString(deliverAt.toEpochMilli()).getBytes(StandardCharsets.UTF_8));
  }

  /** The delivery time in epoch milliseconds; {@code null} when the header is missing or invalid. */
  public static Long deliverAt(Headers headers) {
    String text = text(headers, HeaderNames.DELIVER_AT);
    if (text == null) {
      return null;
    }
    try {
      long millis = Long.parseLong(text);
      return millis >= 0 ? millis : null;
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /** The {@link HeaderNames#CANCEL} header for the scheduled command with this id. */
  public static Header cancel(String commandId) {
    return new RecordHeader(HeaderNames.CANCEL, commandId.getBytes(StandardCharsets.UTF_8));
  }

  /** The id of the scheduled command to cancel; {@code null} when the header is missing or blank. */
  public static String cancel(Headers headers) {
    String commandId = text(headers, HeaderNames.CANCEL);
    return StringUtils.isBlank(commandId) ? null : commandId;
  }

  private static String text(Headers headers, String name) {
    Header header = headers != null ? headers.lastHeader(name) : null;
    return header != null && header.value() != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
  }
}
