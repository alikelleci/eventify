package io.github.alikelleci.eventify.test;

import io.github.alikelleci.eventify.core.command.CommandResult;
import io.github.alikelleci.eventify.core.event.Event;

import java.util.List;
import java.util.function.Consumer;

/** The outcome of a command in an {@link AggregateFixture}. The expectations throw an {@link AssertionError}. */
public final class Then<A> {

  private final CommandResult result;
  private final A state;

  Then(CommandResult result, A state) {
    this.result = result;
    this.state = state;
  }

  /** The command's result, as written to its result topic. */
  public CommandResult result() {
    return result;
  }

  /** The payloads of the command's events in stored order; empty when it was rejected. */
  public List<Object> events() {
    return result instanceof CommandResult.Success success
        ? success.events().stream().map(Event::getPayload).toList()
        : List.of();
  }

  /** The aggregate after the command; {@code null} before it is created or after it is removed. */
  public A state() {
    return state;
  }

  /** The command was accepted. */
  public Then<A> expectSuccess() {
    if (result instanceof CommandResult.Failure failure) {
      throw new AssertionError("Expected " + type() + " to succeed, but it was rejected: " + failure.cause());
    }
    return this;
  }

  /** The command was accepted with exactly these events, in this order. */
  public Then<A> expectEvents(Object... events) {
    expectSuccess();
    List<Object> expected = List.of(events);
    if (!expected.equals(events())) {
      throw new AssertionError("Expected " + type() + " to give events " + expected + ", but it gave " + events() + ".");
    }
    return this;
  }

  /** The command was accepted without events. */
  public Then<A> expectNoEvents() {
    return expectEvents();
  }

  /** The command was rejected, and nothing of it was stored. */
  public Then<A> expectRejection() {
    if (result instanceof CommandResult.Success) {
      throw new AssertionError("Expected " + type() + " to be rejected, but it succeeded with events " + events() + ".");
    }
    return this;
  }

  /** The command was rejected by this exception, as the root cause. */
  public Then<A> expectRejection(Class<? extends Throwable> exception) {
    expectRejection();
    String name = shortName(exception);
    if (cause() == null || !cause().startsWith(name + ":")) {
      throw new AssertionError("Expected " + type() + " to be rejected with " + name + ", but it was rejected with: " + cause());
    }
    return this;
  }

  /** The command was rejected by this exception with this message, as the root cause. */
  public Then<A> expectRejection(Class<? extends Throwable> exception, String message) {
    expectRejection();
    String expected = shortName(exception) + ": " + message;
    if (!expected.equals(cause())) {
      throw new AssertionError("Expected " + type() + " to be rejected with \"" + expected + "\", but it was rejected with: " + cause());
    }
    return this;
  }

  /** Why the command was rejected, as "Exception: message" of the root cause; {@code null} when it was accepted. */
  public String cause() {
    return result instanceof CommandResult.Failure failure ? failure.cause() : null;
  }

  /** Checks the aggregate after the command, {@code null} before it is created or after it is removed. */
  public Then<A> expectState(Consumer<A> assertions) {
    assertions.accept(state);
    return this;
  }

  private String type() {
    return result.command().getType();
  }

  /** As Eventify writes it in the cause: without the package, with dots between nested classes. */
  private static String shortName(Class<?> exception) {
    String packageName = exception.getPackageName();
    return exception.getName().substring(packageName.isEmpty() ? 0 : packageName.length() + 1).replace('$', '.');
  }
}
