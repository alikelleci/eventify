package io.github.alikelleci.eventify.core.support;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.processor.api.Record;

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
public class ReplyExceptionHandler implements ProductionExceptionHandler, ProcessingExceptionHandler {

  /**
   * The name every node that sends the replies starts with, as {@code Eventify.topology()} names them. Sending a reply
   * can fail in the sink itself, or in the node that forwards to it (that is where Kafka Streams looks up the topic's
   * partitions), so all of them carry this prefix.
   */
  public static final String REPLY_NODES = "eventify-reply";
  public static final String REPLY_SINK = REPLY_NODES + "-sink";
  public static final String REPLY_FILTER = REPLY_NODES + "-filter";
  public static final String REPLY_RESULT = REPLY_NODES + "-result";

  @Override
  public ProductionExceptionHandler.Response handleError(ErrorHandlerContext context, ProducerRecord<byte[], byte[]> record, Exception exception) {
    return isReply(context)
        ? dropReply(record, exception)
        : ProductionExceptionHandler.Response.fail();
  }

  @Override
  public ProductionExceptionHandler.Response handleSerializationError(ErrorHandlerContext context, ProducerRecord record, Exception exception, SerializationExceptionOrigin origin) {
    return isReply(context)
        ? dropReply(record, exception)
        : ProductionExceptionHandler.Response.fail();
  }

  /**
   * Sending a reply starts before the record leaves: the topic's partitions are looked up, because a reply goes to
   * partition 0. A topic Kafka refuses (e.g. a name with a space) fails there, in the sink itself.
   */
  @Override
  public ProcessingExceptionHandler.Response handleError(ErrorHandlerContext context, Record<?, ?> record, Exception exception) {
    if (!isReply(context)) {
      return ProcessingExceptionHandler.Response.fail();
    }
    log.warn("The result of a command could not be sent to its reply topic; the command itself was handled. "
        + "Its sender gets no answer and will time out.", exception);
    return ProcessingExceptionHandler.Response.resume();
  }

  private static boolean isReply(ErrorHandlerContext context) {
    return context != null && context.processorNodeId() != null && context.processorNodeId().startsWith(REPLY_NODES);
  }

  private static ProductionExceptionHandler.Response dropReply(ProducerRecord<?, ?> record, Exception exception) {
    log.warn("The result of a command could not be sent to its reply topic '{}'; the command itself was handled. "
            + "Its sender gets no answer and will time out.", record != null ? record.topic() : null, exception);
    return ProductionExceptionHandler.Response.resume();
  }

  @Override
  public void configure(Map<String, ?> configs) {
  }
}
