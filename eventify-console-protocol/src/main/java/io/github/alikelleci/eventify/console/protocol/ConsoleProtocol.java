package io.github.alikelleci.eventify.console.protocol;

/**
 * How an application and the console talk to each other.
 *
 * <p>The application connects to the console over RSocket (on WebSocket) at {@link #RSOCKET_PATH} and sends a
 * {@link NodeInfo} as the setup data, and the console's application token as the setup metadata (UTF-8 text, empty
 * without a token). From then on the console sends requests to the application:
 * <ul>
 *   <li>request metadata: the JSON of a {@link RequestHeader}</li>
 *   <li>request data: the JSON of the route's request (see {@link Route})</li>
 *   <li>response metadata: the JSON of a {@link ReplyHeader}</li>
 *   <li>response data: the JSON body, only when the status is {@link ReplyHeader.Status#OK}</li>
 * </ul>
 * The console passes the response body to the browser as it is: it never needs the application's classes.
 */
public final class ConsoleProtocol {

  /** Raised when the messages change in a way older consoles or applications can't handle. */
  public static final int VERSION = 1;

  public static final String RSOCKET_PATH = "/rsocket";

  public static final String DATA_MIME_TYPE = "application/json";
  public static final String METADATA_MIME_TYPE = "application/json";

  /** WebSocket frames are limited to 64 KB, so both sides send larger messages (a page of events) in parts of this size. */
  public static final int FRAGMENT_SIZE = 16 * 1024;

  /**
   * The largest message either side accepts, once its parts are put together. A larger one is refused instead of read
   * into memory, so one side can't make the other run out of memory, by mistake (a huge aggregate state) or not.
   */
  public static final int MAX_PAYLOAD_SIZE = 16 * 1024 * 1024;

  private ConsoleProtocol() {
  }
}
