package io.github.alikelleci.eventify.console.protocol;

/**
 * What a request is, sent as the request metadata.
 *
 * @param route the {@link Route} name. A name, not the enum, so an application that doesn't know a newer route can
 *              still read the header and answer {@link ReplyHeader.Status#BAD_REQUEST}.
 */
public record RequestHeader(String route) {

  public static RequestHeader of(Route route) {
    return new RequestHeader(route.name());
  }
}
