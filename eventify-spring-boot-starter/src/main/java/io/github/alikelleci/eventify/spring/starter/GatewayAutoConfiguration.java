package io.github.alikelleci.eventify.spring.starter;

import io.github.alikelleci.eventify.GatewayBuilder;
import io.github.alikelleci.eventify.messaging.commandhandling.gateway.CommandGateway;
import io.github.alikelleci.eventify.messaging.eventhandling.gateway.EventGateway;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@ConditionalOnBean(GatewayBuilder.class)
@EnableConfigurationProperties(EventifyProperties.class)
public class GatewayAutoConfiguration {

  @Bean
  public CommandGateway commandGateway(GatewayBuilder builder) {
    return builder
        .commandGateway();
  }

  @Bean
  public EventGateway eventGateway(GatewayBuilder builder) {
    return builder
        .eventGateway();
  }

}
