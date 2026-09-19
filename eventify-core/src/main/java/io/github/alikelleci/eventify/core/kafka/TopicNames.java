package io.github.alikelleci.eventify.core.kafka;

/** The topics Eventify writes to besides the ones named with {@code @Topic}. */
public final class TopicNames {

  private static final String RESULTS_SUFFIX = ".results";

  private TopicNames() {
  }

  /** Where the result of every handled command is written: the command topic with ".results". */
  public static String resultTopicOf(String commandTopic) {
    return commandTopic + RESULTS_SUFFIX;
  }
}
