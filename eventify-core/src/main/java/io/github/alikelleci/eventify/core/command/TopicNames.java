package io.github.alikelleci.eventify.core.command;

/** The topics Eventify writes to besides the ones named with {@code @Topic}. */
public final class TopicNames {

  private static final String RESULTS_SUFFIX = ".results";
  private static final String SCHEDULED_SUFFIX = ".scheduled";

  private TopicNames() {
  }

  /** Where the result of every handled command is written: the command topic with ".results". */
  public static String resultTopicOf(String commandTopic) {
    return commandTopic + RESULTS_SUFFIX;
  }

  /** Where scheduled commands and their cancels are sent: the command topic with ".scheduled". */
  public static String scheduledTopicOf(String commandTopic) {
    return commandTopic + SCHEDULED_SUFFIX;
  }
}
