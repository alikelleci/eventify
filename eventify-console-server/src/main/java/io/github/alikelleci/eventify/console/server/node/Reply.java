package io.github.alikelleci.eventify.console.server.node;

import io.github.alikelleci.eventify.console.protocol.ReplyHeader;

/** An application's answer: the outcome and, when OK, the JSON body as the application wrote it. */
public record Reply(ReplyHeader header, byte[] body) {

  public static Reply of(ReplyHeader header) {
    return new Reply(header, new byte[0]);
  }
}
