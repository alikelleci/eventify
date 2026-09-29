package io.github.alikelleci.eventify.migration;

import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/** What the check found. The migration may only be written when there are no conflicts. */
final class CheckReport {

  private static final int MAX_LISTED = 50;

  final String aggregateType;
  final String applicationId;
  final String eventTopic;
  final String snapshotTopic;
  int eventPartitions;
  int snapshotPartitions;
  long events;
  long migratedEvents;
  long aggregates;
  long largestPartitionEvents;
  String largestAggregate;
  long largestAggregateEvents;
  long v4Snapshots;
  boolean dropSnapshots;
  final Map<String, Long> eventClasses = new TreeMap<>();
  final List<SharedRange> sharedRanges = new ArrayList<>();
  final List<String> conflicts = new ArrayList<>();
  long conflictCount;
  Duration duration = Duration.ZERO;

  CheckReport(String aggregateType, String applicationId, boolean dropSnapshots) {
    this.aggregateType = aggregateType;
    this.applicationId = applicationId;
    this.eventTopic = applicationId + "-event-store-changelog";
    this.snapshotTopic = applicationId + "-snapshot-store-changelog";
    this.dropSnapshots = dropSnapshots;
  }

  boolean hasConflicts() {
    return conflictCount > 0;
  }

  /** Every event is under its Eventify 5 key and no Eventify 4 snapshot is left: Eventify 5 may start. */
  boolean isComplete() {
    return !hasConflicts() && eventPartitions > 0 && migratedEvents == events && v4Snapshots == 0;
  }

  void conflict(String description) {
    if (conflictCount++ < MAX_LISTED) {
      conflicts.add(description);
    }
  }

  void print(PrintStream out, String title) {
    out.println(title + ": Eventify 4 -> 5, application " + applicationId + ", aggregate type \"" + aggregateType + "\"");
    out.println();
    out.println("Event store    " + eventTopic + " (" + eventPartitions + " partitions)");
    out.println("  " + number(events) + " events in " + number(aggregates) + " aggregates"
        + (largestAggregate == null ? "" : ", the largest is " + largestAggregate + " with " + number(largestAggregateEvents)));
    out.println("  " + number(events - migratedEvents) + " still under an Eventify 4 key, " + number(migratedEvents) + " migrated");
    out.println("  the largest partition holds " + number(largestPartitionEvents) + " records: give the tool at least "
        + (largestPartitionEvents / 1_000_000 + 1) + " GB of heap (java -Xmx" + (largestPartitionEvents / 1_000_000 + 1) + "g -jar ...)");
    if (largestAggregate != null) {
      out.println("  keys become   " + Keys.printable(Keys.v5Event(aggregateType, largestAggregate, 1)) + " and on");
    }
    out.println("  event classes (all of them must belong to aggregate type \"" + aggregateType + "\"):");
    eventClasses.forEach((type, count) -> out.println("    " + pad(number(count), 12) + "  " + type));
    out.println();
    out.println("Snapshot store " + snapshotTopic + " (" + snapshotPartitions + " partitions)");
    out.println("  " + number(v4Snapshots) + " Eventify 4 snapshots" + (v4Snapshots > 0 && dropSnapshots ? ", to be deleted" : ""));
    out.println();
    if (!sharedRanges.isEmpty()) {
      out.println("Aggregates whose Eventify 4 state also included another aggregate's events (they will no longer):");
      sharedRanges.forEach(shared -> out.println("  " + shared.aggregateId() + " included " + number(shared.events())
          + " events of " + shared.otherAggregateId()));
      out.println();
    }
    if (hasConflicts()) {
      out.println("CONFLICTS: " + number(conflictCount) + ". Nothing is written while there are conflicts.");
      conflicts.forEach(conflict -> out.println("  " + conflict));
      if (conflictCount > conflicts.size()) {
        out.println("  ... and " + number(conflictCount - conflicts.size()) + " more");
      }
    } else {
      out.println("No conflicts." + (isComplete() ? " The migration is complete." : ""));
    }
    out.println("Read in " + duration.toSeconds() + " s.");
  }

  private static String number(long value) {
    return String.format(Locale.ROOT, "%,d", value);
  }

  private static String pad(String value, int width) {
    return " ".repeat(Math.max(0, width - value.length())) + value;
  }

  /** In Eventify 4 the range of {@code aggregateId} ({@code id@} up to {@code id@~}) also held {@code otherAggregateId}. */
  record SharedRange(String aggregateId, String otherAggregateId, long events) {
  }
}
