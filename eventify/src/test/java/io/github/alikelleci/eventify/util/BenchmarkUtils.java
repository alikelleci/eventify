package io.github.alikelleci.eventify.util;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

@Slf4j
public class BenchmarkUtils {

  public static <T> T logExecutionTime(Supplier<T> method) {
    Instant startTime = Instant.now();
    T result = method.get();
    Instant endTime = Instant.now();

    Duration duration = Duration.between(startTime, endTime);

    log.info("Execution time: {} ms ({} sec)", duration.toMillis(), duration.toSeconds());
    return result;
  }

  public static void logExecutionTime(Runnable method) {
    Instant startTime = Instant.now();
    method.run();
    Instant endTime = Instant.now();

    Duration duration = Duration.between(startTime, endTime);

    log.info("Execution time: {} ms ({} sec)", duration.toMillis(), duration.toSeconds());
  }

}
