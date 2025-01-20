package io.github.alikelleci.eventify.spring.starter;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "eventify")
public class EventifyProperties {
//  private String bootstrapServers;
//  private String applicationId;
}
