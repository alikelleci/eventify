package io.github.alikelleci.eventify.experimental;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.common.annotations.AggregateId;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.eventify.util.JacksonUtils;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;

public class Test {

  public static ObjectMapper objectMapper = JacksonUtils.enhancedObjectMapper();

  public static void main(String[] args) throws JsonProcessingException {


    Command command = Command.builder()
        .timestamp(Instant.ofEpochMilli(1584709244000L))
        .metadata(new Metadata().add("aaa", "bbb"))
        .payload(Customer.builder()
            .id("cust-1")
            .name("Henk")
            .build())
        .build();

    String json = objectMapper.writeValueAsString(command);
    System.out.println(json);

    System.out.println(command);

    Command c = objectMapper.readValue(json, Command.class);
    System.out.println(c);

    System.out.println("---------------");


    Aggregate aggregate = Aggregate.builder()
        .timestamp(Instant.ofEpochMilli(1584709244000L))
        .metadata(new Metadata().add("aaa", "bbb"))
        .payload(Customer.builder()
            .id("cust-1")
            .name("Henk")
            .build())
        .build();

    System.out.println(aggregate);




  }

  @Value
  @Builder
  static class Customer {
    @AggregateId
    private String id;
    private String name;

  }
}
