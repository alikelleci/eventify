package io.github.alikelleci.eventify.migration;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Moves the stores of an Eventify 4 application to the Eventify 5 format.
 *
 * <pre>
 * java -jar eventify-migration.jar check|migrate|verify --bootstrap-servers host:9092 --application-id my-app
 *     --aggregate-type order [--config client.properties] [--drop-snapshots]
 * java -jar eventify-migration.jar backup|restore --bootstrap-servers host:9092 --application-id my-app
 *     [--config client.properties]
 * </pre>
 *
 * Exit code 0: done (check: no conflicts; migrate and verify: complete); 1: see the report; 2: wrong arguments.
 */
public final class MigrationTool {

  private static final String USAGE = """
      Usage: check|migrate|verify --bootstrap-servers <servers> --application-id <id> --aggregate-type <name>
                   [--config <client.properties>] [--drop-snapshots]
             backup|restore --bootstrap-servers <servers> --application-id <id> [--config <client.properties>]

        check              read both stores and report what the migration would do; writes nothing
        backup             copy both Eventify 4 stores to <changelog>-v4-backup; before migrate, application stopped
        migrate            check, write, and verify; only while every instance of the application is stopped
        verify             read both stores and report whether the migration is complete; writes nothing
        restore            put the backup back, for a return to Eventify 4; application stopped
        --application-id   the application.id of the Eventify 4 application; it names the changelog topics
        --aggregate-type   the @AggregateRoot name of the aggregate in the Eventify 5 application
        --config           Kafka client settings, e.g. security.protocol and sasl.jaas.config
        --drop-snapshots   delete the Eventify 4 snapshots; only when they are a cache (no deleteEvents)
      """;

  private MigrationTool() {
  }

  public static void main(String[] args) {
    System.exit(run(args));
  }

  static int run(String[] args) {
    Map<String, String> options;
    try {
      options = options(args);
    } catch (IllegalArgumentException e) {
      System.err.println(e.getMessage());
      System.err.print(USAGE);
      return 2;
    }
    Properties config = new Properties();
    try {
      if (options.containsKey("--config")) {
        try (InputStream in = Files.newInputStream(Path.of(options.get("--config")))) {
          config.load(in);
        }
      }
    } catch (IOException e) {
      System.err.println("Cannot read " + options.get("--config") + ": " + e.getMessage());
      return 2;
    }
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.get("--bootstrap-servers"));

    String command = args[0];
    String applicationId = options.get("--application-id");
    if (command.equals("backup") || command.equals("restore")) {
      try {
        Backup backup = new Backup(config, applicationId);
        (command.equals("backup") ? backup.backup() : backup.restore()).forEach(System.out::println);
        return 0;
      } catch (IllegalStateException e) {
        System.out.println("STOPPED: " + e.getMessage());
        return 1;
      }
    }
    MigrationCheck check = new MigrationCheck(config, options.get("--aggregate-type"), options.containsKey("--drop-snapshots"));
    CheckReport report = check.run(applicationId);
    if (command.equals("verify")) {
      report.print(System.out, "Verify");
      return report.isComplete() ? 0 : 1;
    }
    report.print(System.out, "Check");
    if (command.equals("check") || report.hasConflicts()) {
      return report.hasConflicts() ? 1 : 0;
    }

    System.out.println();
    try {
      long written = new MigrationWrite(config, check, options.get("--aggregate-type")).run(report);
      System.out.println("Wrote " + written + " events under their Eventify 5 key.");
    } catch (IllegalStateException e) {
      System.out.println("STOPPED: " + e.getMessage());
      return 1;
    }
    System.out.println();
    CheckReport verified = check.run(applicationId);
    verified.print(System.out, "Verify");
    return verified.isComplete() ? 0 : 1;
  }

  private static Map<String, String> options(String[] args) {
    if (args.length == 0 || !List.of("check", "backup", "migrate", "verify", "restore").contains(args[0])) {
      throw new IllegalArgumentException(args.length == 0 ? "No command given." : "Unknown command: " + args[0]);
    }
    Map<String, String> options = new HashMap<>();
    for (int i = 1; i < args.length; i++) {
      switch (args[i]) {
        case "--drop-snapshots" -> options.put(args[i], "");
        case "--bootstrap-servers", "--application-id", "--aggregate-type", "--config" -> {
          if (i + 1 == args.length) {
            throw new IllegalArgumentException(args[i] + " needs a value.");
          }
          options.put(args[i], args[++i]);
        }
        default -> throw new IllegalArgumentException("Unknown option: " + args[i]);
      }
    }
    List<String> required = args[0].equals("backup") || args[0].equals("restore")
        ? List.of("--bootstrap-servers", "--application-id")
        : List.of("--bootstrap-servers", "--application-id", "--aggregate-type");
    for (String option : required) {
      if (!options.containsKey(option)) {
        throw new IllegalArgumentException(option + " is required.");
      }
    }
    return options;
  }
}
