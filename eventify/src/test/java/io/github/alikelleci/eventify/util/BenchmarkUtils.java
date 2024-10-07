package io.github.alikelleci.eventify.util;

import lombok.extern.slf4j.Slf4j;

import java.util.function.Supplier;

@Slf4j
public class BenchmarkUtils {

  public static <T> T logExecutionTime(Supplier<T> method) {
    long startTime = System.currentTimeMillis();
    T result = method.get();
    long endTime = System.currentTimeMillis();

    long duration = endTime - startTime;
    double durationInSec = duration / 1000.0;

    log.info("Execution time: {} ms ({} sec)", duration, ((int) durationInSec));
    return result;
  }

  public static void logExecutionTime(Runnable method) {
    long startTime = System.currentTimeMillis();
    method.run();
    long endTime = System.currentTimeMillis();

    long duration = endTime - startTime;
    double durationInSec = duration / 1000.0;

    log.info("Execution time: {} ms ({} sec)", duration, ((int) durationInSec));
  }
}
