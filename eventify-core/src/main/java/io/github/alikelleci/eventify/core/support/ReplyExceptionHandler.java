package io.github.alikelleci.eventify.core.support;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

import java.util.Map;

/**
 * What Eventify does when a record cannot be sent.
 *
 * <p>Everything Eventify sends is part of the transaction of the command that produced it, and a failure fails the
 * task: the transaction is aborted and nothing of that command is committed. That is what should happen for an event
 * or a command result.
 *
 * <p>A reply is different. Its topic is named by whoever sent the command ({@code $replyTo}), so it may not exist, or
 * this application may not be allowed to write to it. Failing on it would stop this instance, and the command would be
 * handled again after the restart, and fail again, on every instance in turn: one sender with a wrong reply topic
 * would stop command handling for everyone. The reply is dropped instead, which costs that one sender its answer: it
 * waits for its result until it times out.
 */
@Slf4j
public class ReplyExceptionHandler implements ProductionExceptionHandler {

  /** The name of the sink that sends the replies, as {@code Eventify.topology()} names it. */
  public static final String REPLY_SINK = "eventify-reply-sink";

  @Override
  public Response handleError(ErrorHandlerContext context, ProducerRecord<byte[], byte[]> record, Exception exception) {
    return isReply(context)
        ? dropReply(record, exception)
        : Response.fail();
  }

  @Override
  public Response handleSerializationError(ErrorHandlerContext context, ProducerRecord record, Exception exception, SerializationExceptionOrigin origin) {
    return isReply(context)
        ? dropReply(record, exception)
        : Response.fail();
  }

  private static boolean isReply(ErrorHandlerContext context) {
    return context != null && REPLY_SINK.equals(context.processorNodeId());
  }

  private static Response dropReply(ProducerRecord<?, ?> record, Exception exception) {
    log.warn("The result of a command could not be sent to its reply topic '{}'; the command itself was handled. "
            + "Its sender gets no answer and will time out.", record != null ? record.topic() : null, exception);
    return Response.resume();
  }

  @Override
  public void configure(Map<String, ?> configs) {
  }
}
