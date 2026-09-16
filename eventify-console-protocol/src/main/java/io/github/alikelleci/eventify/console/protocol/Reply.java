package io.github.alikelleci.eventify.console.protocol;

/** An application's answer to a request: the outcome and, when {@link ReplyHeader.Status#OK}, the JSON body. */
public record Reply(ReplyHeader header, byte[] body) {

  public static Reply of(ReplyHeader header) {
    return new Reply(header, new byte[0]);
  }
}
