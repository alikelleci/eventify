package io.github.alikelleci.eventify.console.protocol;

/**
 * The requests the console sends to an application, with the JSON each one carries.
 *
 * <p>A route that answers with a list answers an empty list when there is nothing, e.g. for an aggregate without
 * events. A route that answers with one thing answers {@link ReplyHeader.Status#NOT_FOUND} when it isn't there.
 */
public enum Route {

  /** {@link Requests.Events} */
  EVENTS(Target.OWNER),
  /** {@link Requests.EventDetail} */
  EVENT_DETAIL(Target.OWNER),
  /** {@link Requests.EventsOfCommand} */
  EVENTS_OF_COMMAND(Target.OWNER),
  /** {@link Requests.State} */
  STATE(Target.OWNER),
  /** {@link Requests.Commands} */
  COMMANDS(Target.ANY),
  /** The command to retry, as JSON. */
  RETRY_COMMAND(Target.ANY),
  /** No request data; answered with {@link NodeStatus}. */
  STATUS(Target.INSTANCE);

  /** Which instance of the application a request goes to. */
  public enum Target {
    /**
     * The instance that owns the aggregate: it reads the local state store. An instance that doesn't own it replies
     * {@link ReplyHeader.Status#NOT_OWNER}.
     */
    OWNER,
    /** Any one instance: they all give the same answer. */
    ANY,
    /** One chosen instance, which answers about itself. */
    INSTANCE
  }

  private final Target target;

  Route(Target target) {
    this.target = target;
  }

  public Target target() {
    return target;
  }
}
