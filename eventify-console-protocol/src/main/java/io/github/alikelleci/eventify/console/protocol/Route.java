package io.github.alikelleci.eventify.console.protocol;

/** The requests the console sends to an application, with the JSON each one carries. */
public enum Route {

  /** {@link Requests.Events} */
  EVENTS(true),
  /** {@link Requests.EventDetail} */
  EVENT_DETAIL(true),
  /** {@link Requests.EventsByCorrelation} */
  EVENTS_BY_CORRELATION(true),
  /** {@link Requests.State} */
  STATE(true),
  /** {@link Requests.Commands} */
  COMMANDS(false),
  /** The command to retry, as JSON. */
  RETRY_COMMAND(false),
  /** No request data; answered with {@link InstanceStatus}. Every instance answers for itself. */
  STATUS(false);

  private final boolean ownerRouted;

  Route(boolean ownerRouted) {
    this.ownerRouted = ownerRouted;
  }

  /**
   * Whether only the instance that owns the aggregate can answer (it reads the local state store). An instance that
   * doesn't own it replies {@link ReplyHeader.Status#NOT_OWNER}. Other routes can be answered by any one instance.
   */
  public boolean ownerRouted() {
    return ownerRouted;
  }
}
