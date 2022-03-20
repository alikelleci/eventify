package io.github.alikelleci.eventify.experimental;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.common.annotations.AggregateId;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.eventify.util.JacksonUtils;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;

public class Test {

  public static ObjectMapper objectMapper = JacksonUtils.enhancedObjectMapper();

  public static void main(String[] args) throws JsonProcessingException {

    Customer customer = Customer.builder()
        .id("cust-1")
        .name("Henk")
        .build();


    Command command = Command.builder()
        .payload(customer)
        .timestamp(Instant.ofEpochMilli(1584709244000L))
        .build();
    System.out.println(command);

    Event event = Event.builder()
        .payload(customer)
        .timestamp(Instant.ofEpochMilli(1584709244000L))
        .build();
    System.out.println(event);


    Aggregate aggregate = Aggregate.builder()
        .payload(customer)
        .timestamp(Instant.ofEpochMilli(1584709244000L))
        .eventId(event.getId())
        .version(12)
        .build();
    System.out.println(aggregate);

    System.out.println("---------------");

    String json = objectMapper.writeValueAsString(command);
    System.out.println(json);

    Command c = objectMapper.readValue(json, Command.class);
    System.out.println(c);
  }

  @Value
  @Builder
  static class Customer {
    @AggregateId
    private String id;
    private String name;

  }
}
