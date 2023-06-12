package com.linkedin.venice.utils;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.function.Supplier;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.event.ExecutionAttemptedEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class RetryUtils {
  private static final Logger LOGGER = LogManager.getLogger(RetryUtils.class);

  private RetryUtils() {
    // Util class
  }

  /**
   * Execute a {@link Runnable} with fixed delay with a maximum attempt. If all attempts are made and still no success,
   * the last thrown exception will be thrown. This function logs the throwable that failed intermediate execution attempts.
   *
   * @param runnable Execution unit which returns nothing ({@link Void})
   * @param maxAttempt Total maximum attempts made before giving up. Value should be at least one.
   * @param delay Fixed delay between retry attempts.
   * @param retryFailureTypes Types of failures upon which retry attempt is made. If a failure with type not specified
   *                          in this list is thrown, it gets thrown to the caller.
   */
  public static void executeWithMaxAttempt(
      VeniceCheckedRunnable runnable,
      final int maxAttempt,
      Duration delay,
      List<Class<? extends Throwable>> retryFailureTypes) {
    executeWithMaxAttempt(runnable, maxAttempt, delay, retryFailureTypes, RetryUtils::logAttemptWithFailure);
  }

  /**
   * Execute a {@link Runnable} with fixed delay with a maximum attempt. If all attempts are made and still no success,
   * the last thrown exception will be thrown. This function does not log the throwable that failed intermediate execution attempts.
   *
   * @param runnable Execution unit which returns nothing ({@link Void})
   * @param maxAttempt Total maximum attempts made before giving up. Value should be at least one.
   * @param delay Fixed delay between retry attempts.
   * @param retryFailureTypes Types of failures upon which retry attempt is made. If a failure with type not specified
   *                          in this list is thrown, it gets thrown to the caller.
   */
  public static void executeWithMaxAttemptNoIntermediateLogging(
      VeniceCheckedRunnable runnable,
      final int maxAttempt,
      Duration delay,
      List<Class<? extends Throwable>> retryFailureTypes) {
    executeWithMaxAttempt(runnable, maxAttempt, delay, retryFailureTypes, RetryUtils::doNotLog);
  }

  /**
   * Execute a {@link Runnable} with fixed delay with a maximum attempt. If all attempts are made and still no success,
   * the last thrown exception will be thrown.
   *
   * @param runnable Execution unit which returns nothing ({@link Void})
   * @param maxAttempt Total maximum attempts made before giving up. Value should be at least one.
   * @param delay Fixed delay between retry attempts.
   * @param retryFailureTypes Types of failures upon which retry attempt is made. If a failure with type not specified
   *                          in this list is thrown, it gets thrown to the caller.
   * @param intermediateFailureHandler A handler for intermediate failure(s).
   */
  public static void executeWithMaxAttempt(
      VeniceCheckedRunnable runnable,
      final int maxAttempt,
      Duration delay,
      List<Class<? extends Throwable>> retryFailureTypes,
      IntermediateFailureHandler intermediateFailureHandler) {
    RetryPolicy<Object> retryPolicy =
        new RetryPolicy<>().handle(retryFailureTypes).withDelay(delay).withMaxAttempts(maxAttempt);

    retryPolicy.onFailedAttempt(intermediateFailureHandler::handle);
    Failsafe.with(retryPolicy).run(runnable::run);
  }

  /**
   * Execute a {@link Runnable} with exponential backoff. If all attempts are made or the max duration has reached
   * and still no success, the last thrown exception will be thrown. This function logs the throwable that failed
   * intermediate execution attempts.
   *
   * @param runnable Execution unit which returns nothing ({@link Void})
   * @param maxAttempt Total maximum attempts made before giving up. Value should be at least one.
   * @param initialDelay First delay duration.
   * @param maxDelay Maximum delay duration.
   * @param maxDuration Maximum total execution.
   * @param retryFailureTypes Types of failures upon which retry attempt is made. If a failure with type not specified
   *                           in this list is thrown, it gets thrown to the caller.
   */
  public static void executeWithMaxAttemptAndExponentialBackoff(
      VeniceCheckedRunnable runnable,
      final int maxAttempt,
      Duration initialDelay,
      Duration maxDelay,
      Duration maxDuration,
      List<Class<? extends Throwable>> retryFailureTypes) {
    executeWithMaxAttemptAndExponentialBackoff(
        runnable,
        maxAttempt,
        initialDelay,
        maxDelay,
        maxDuration,
        retryFailureTypes,
        RetryUtils::logAttemptWithFailure);
  }

  /**
   * Execute a {@link Runnable} with exponential backoff. If all attempts are made or the max duration has reached
   * and still no success, the last thrown exception will be thrown.
   *
   * @param runnable Execution unit which returns nothing ({@link Void})
   * @param maxAttempt Total maximum attempts made before giving up. Value should be at least one.
   * @param initialDelay First delay duration.
   * @param maxDelay Maximum delay duration.
   * @param maxDuration Maximum total execution.
   * @param retryFailureTypes Types of failures upon which retry attempt is made. If a failure with type not specified
   *                           in this list is thrown, it gets thrown to the caller.
   * @param intermediateFailureHandler A handler for intermediate failure(s).
   */
  public static void executeWithMaxAttemptAndExponentialBackoff(
      VeniceCheckedRunnable runnable,
      final int maxAttempt,
      Duration initialDelay,
      Duration maxDelay,
      Duration maxDuration,
      List<Class<? extends Throwable>> retryFailureTypes,
      IntermediateFailureHandler intermediateFailureHandler) {
    RetryPolicy<Object> retryPolicy = new RetryPolicy<>().handle(retryFailureTypes)
        .withBackoff(initialDelay.toMillis(), maxDelay.toMillis(), ChronoUnit.MILLIS, 2.0)
        .withMaxDuration(maxDuration)
        .withMaxAttempts(maxAttempt);

    retryPolicy.onFailedAttempt(intermediateFailureHandler::handle);
    Failsafe.with(retryPolicy).run(runnable::run);
  }

  /**
   * Execute a {@link Supplier} with fixed delay with a maximum attempt. If all attempts are made and still no success,
   * the last thrown exception will be thrown. This function logs the throwable that failed intermediate execution attempts.
   *
   * @param supplier Execution unit which returns something ({@link T})
   * @param maxAttempt Total maximum attempts made before giving up. Value should be at least one.
   * @param delay Fixed delay between retry attempts.
   * @param retryFailureTypes Types of failures upon which retry attempt is made. If a failure with type not specified
   *                          in this list is thrown, it gets thrown to the caller.
   */
  public static <T> T executeWithMaxAttempt(
      VeniceCheckedSupplier<T> supplier,
      final int maxAttempt,
      Duration delay,
      List<Class<? extends Throwable>> retryFailureTypes) {
    return executeWithMaxAttempt(supplier, maxAttempt, delay, retryFailureTypes, RetryUtils::logAttemptWithFailure);
  }

  /**
   * Execute a {@link Supplier} with fixed delay with a maximum attempt. If all attempts are made and still no success,
   * the last thrown exception will be thrown.
   *
   * @param supplier Execution unit which returns something ({@link T})
   * @param maxAttempt Total maximum attempts made before giving up. Value should be at least one.
   * @param delay Fixed delay between retry attempts.
   * @param retryFailureTypes Types of failures upon which retry attempt is made. If a failure with type not specified
   *                          in this list is thrown, it gets thrown to the caller.
   * @param intermediateFailureHandler A handler for intermediate failure(s).
   */
  public static <T> T executeWithMaxAttempt(
      VeniceCheckedSupplier<T> supplier,
      final int maxAttempt,
      Duration delay,
      List<Class<? extends Throwable>> retryFailureTypes,
      IntermediateFailureHandler intermediateFailureHandler) {
    RetryPolicy<Object> retryPolicy =
        new RetryPolicy<>().handle(retryFailureTypes).withDelay(delay).withMaxAttempts(maxAttempt);

    retryPolicy.onFailedAttempt(intermediateFailureHandler::handle);
    return Failsafe.with(retryPolicy).get(supplier::get);
  }

  /**
   * Execute a {@link Supplier} with exponential backoff. If all attempts are made or the max duration has reached
   * and still no success, the last thrown exception will be thrown. This function logs the throwable that failed
   * intermediate execution attempts.
   *
   * @param supplier Execution unit which returns something ({@link T})
   * @param maxAttempt Total maximum attempts made before giving up. Value should be at least one.
   * @param initialDelay First delay duration.
   * @param maxDelay Maximum delay duration.
   * @param maxDuration Maximum total execution.
   * @param retryFailureTypes Types of failures upon which retry attempt is made. If a failure with type not specified
   *                          in this list is thrown, it gets thrown to the caller.
   */
  public static <T> T executeWithMaxAttemptAndExponentialBackoff(
      VeniceCheckedSupplier<T> supplier,
      final int maxAttempt,
      Duration initialDelay,
      Duration maxDelay,
      Duration maxDuration,
      List<Class<? extends Throwable>> retryFailureTypes) {
    return executeWithMaxAttemptAndExponentialBackoff(
        supplier,
        maxAttempt,
        initialDelay,
        maxDelay,
        maxDuration,
        retryFailureTypes,
        RetryUtils::logAttemptWithFailure);
  }

  /**
   * Execute a {@link Supplier} with exponential backoff. If all attempts are made or the max duration has reached
   * and still no success, the last thrown exception will be thrown.
   *
   * @param supplier Execution unit which returns something ({@link T})
   * @param maxAttempt Total maximum attempts made before giving up. Value should be at least one.
   * @param initialDelay First delay duration.
   * @param maxDelay Maximum delay duration.
   * @param maxDuration Maximum total execution.
   * @param retryFailureTypes Types of failures upon which retry attempt is made. If a failure with type not specified
   *                          in this list is thrown, it get thrown to the caller.
   * @param intermediateFailureHandler A handler for intermediate failure(s).
   */
  public static <T> T executeWithMaxAttemptAndExponentialBackoff(
      VeniceCheckedSupplier<T> supplier,
      final int maxAttempt,
      Duration initialDelay,
      Duration maxDelay,
      Duration maxDuration,
      List<Class<? extends Throwable>> retryFailureTypes,
      IntermediateFailureHandler intermediateFailureHandler) {
    RetryPolicy<Object> retryPolicy = new RetryPolicy<>().handle(retryFailureTypes)
        .withBackoff(initialDelay.toMillis(), maxDelay.toMillis(), ChronoUnit.MILLIS, 2.0)
        .withMaxDuration(maxDuration)
        .withMaxAttempts(maxAttempt);

    retryPolicy.onFailedAttempt(intermediateFailureHandler::handle);
    return Failsafe.with(retryPolicy).get(supplier::get);
  }

  /**
   * Execute a {@link Supplier} with maximum retries and fix duration per retry attempt. If all attempts are made and
   * still no success, the last thrown exception will be thrown.
   *
   * @param supplier Execution unit which returns something ({@link T})
   * @param maxRetry Total maximum retries made before giving up.
   * @param durationPerAttempt Total duration per attempt. The delay after attempt will be the (duration per attempt - time spent in the current attempt).
   * @param retryFailureTypes Types of failures upon which retry attempt is made. If a failure with type not specified
   *                          in this list is thrown, it is thrown to the caller.
   */
  public static <T> T executeWithMaxRetriesAndFixedAttemptDuration(
      VeniceCheckedSupplier<T> supplier,
      final int maxRetry,
      Duration durationPerAttempt,
      List<Class<? extends Throwable>> retryFailureTypes) {
    RetryPolicy<Object> retryPolicy = new RetryPolicy<>().handle(retryFailureTypes)
        .withDelay(
            (result, failure, context) -> durationPerAttempt.compareTo(context.getElapsedAttemptTime()) > 0
                ? durationPerAttempt.minus(context.getElapsedAttemptTime())
                : Duration.ZERO)
        .withMaxRetries(maxRetry);
    retryPolicy.onFailedAttempt(RetryUtils::logAttemptWithFailure);
    return Failsafe.with(retryPolicy).get(supplier::get);
  }

  private static <T> void logAttemptWithFailure(ExecutionAttemptedEvent<T> executionAttemptedEvent) {
    LOGGER.error(
        "Execution failed with message {} on attempt count {}",
        executionAttemptedEvent.getLastFailure().getMessage(),
        executionAttemptedEvent.getAttemptCount());
  }

  private static <T> void doNotLog(ExecutionAttemptedEvent<T> executionAttemptedEvent) {
  }

  @FunctionalInterface
  public interface IntermediateFailureHandler {
    void handle(ExecutionAttemptedEvent<?> executionAttemptedEvent);
  }
}
