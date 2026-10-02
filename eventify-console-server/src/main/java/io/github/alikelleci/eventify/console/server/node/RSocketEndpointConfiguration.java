package io.github.alikelleci.eventify.console.server.node;

import io.github.alikelleci.eventify.console.protocol.ConsoleProtocol;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.server.WebsocketRouteTransport;
import org.springframework.boot.reactor.netty.NettyRouteProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** The RSocket endpoint the applications connect to: a WebSocket on the same port as the UI and the API. */
@Configuration
public class RSocketEndpointConfiguration {

  @Bean
  public NettyRouteProvider rsocketRoute(NodeAcceptor acceptor) {
    var connectionAcceptor = RSocketServer.create(acceptor)
        .fragment(ConsoleProtocol.FRAGMENT_SIZE)
        .maxInboundPayloadSize(ConsoleProtocol.MAX_PAYLOAD_SIZE)
        .asConnectionAcceptor();
    return routes -> routes.ws(ConsoleProtocol.RSOCKET_PATH, WebsocketRouteTransport.newHandler(connectionAcceptor));
  }
}
