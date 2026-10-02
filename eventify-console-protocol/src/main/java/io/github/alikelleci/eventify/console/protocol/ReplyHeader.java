package io.github.alikelleci.eventify.console.protocol;

/**
 * The outcome of a request, sent as the response metadata.
 *
 * @param status the outcome
 * @param owner  for {@link Status#NOT_OWNER}: the {@link NodeInfo#nodeId()} of the instance that owns the aggregate
 * @param reason for {@link Status#UNAVAILABLE}, {@link Status#BAD_REQUEST} and {@link Status#UNREADABLE}: why, readable
 *               for people
 */
public record ReplyHeader(Status status, String owner, String reason) {

  public enum Status {
    OK,
    NOT_FOUND,
    /** Another instance owns the aggregate: ask {@link #owner()}. */
    NOT_OWNER,
    /** Can't answer right now, e.g. Kafka Streams is starting or rebalancing. Worth trying again. */
    UNAVAILABLE,
    BAD_REQUEST,
    /** The stored data can't be answered from, e.g. an aggregate's events are incomplete. Trying again won't help. */
    UNREADABLE
  }

  public static ReplyHeader ok() {
    return new ReplyHeader(Status.OK, null, null);
  }

  public static ReplyHeader notFound() {
    return new ReplyHeader(Status.NOT_FOUND, null, null);
  }

  public static ReplyHeader notOwner(String owner) {
    return new ReplyHeader(Status.NOT_OWNER, owner, null);
  }

  public static ReplyHeader unavailable(String reason) {
    return new ReplyHeader(Status.UNAVAILABLE, null, reason);
  }

  public static ReplyHeader badRequest(String reason) {
    return new ReplyHeader(Status.BAD_REQUEST, null, reason);
  }

  public static ReplyHeader unreadable(String reason) {
    return new ReplyHeader(Status.UNREADABLE, null, reason);
  }
}
