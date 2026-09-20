package io.github.alikelleci.eventify.console.protocol;

import java.util.Set;

/**
 * Sent by an application instance when it connects.
 *
 * @param applicationId   the Kafka Streams application id; instances with the same id form one application
 * @param nodeId          the instance's unique name, as in {@code application.server}; {@link ReplyHeader#owner()} refers to it
 * @param hostname        where the instance runs, for display only
 * @param version         the version of eventify-console-client, from its jar; {@code null} when it isn't run from a
 *                        jar, e.g. from an IDE
 * @param protocolVersion the {@link ConsoleProtocol#VERSION} the instance speaks
 * @param aggregateTypes  the aggregates this instance handles, as their {@code @AggregateRoot} names them: an
 *                        aggregate is addressed by its type and its identifier together
 */
public record NodeInfo(String applicationId, String nodeId, String hostname, String version, int protocolVersion,
                       Set<String> aggregateTypes) {
}
