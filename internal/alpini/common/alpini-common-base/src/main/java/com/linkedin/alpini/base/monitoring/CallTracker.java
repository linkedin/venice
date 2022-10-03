package com.linkedin.alpini.base.monitoring;

import com.linkedin.alpini.base.misc.ExceptionUtil;
import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.base.statistics.LongStats;
import java.util.concurrent.TimeUnit;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface CallTracker {
  Exception GENERIC_EXCEPTION = ExceptionUtil.withoutStackTrace(new Exception());

  @Nonnull
  @CheckReturnValue
  default CallCompletion startCall() {
    return startCall(Time.nanoTime());
  }

  @Nonnull
  @CheckReturnValue
  CallCompletion startCall(@Nonnegative long startTimeNanos);

  default void trackCall(long duration) {
    trackCall(duration, TimeUnit.MILLISECONDS);
  }

  default void trackCall(long duration, @Nonnull TimeUnit timeUnit) {
    trackCallWithError(duration, timeUnit, null);
  }

  default void trackCallWithError(long duration) {
    trackCallWithError(duration, TimeUnit.MILLISECONDS);
  }

  default void trackCallWithError(long duration, @Nonnull TimeUnit timeUnit) {
    trackCallWithError(duration, timeUnit, GENERIC_EXCEPTION);
  }

  default void trackCallWithError(long duration, Throwable throwable) {
    trackCallWithError(duration, TimeUnit.MILLISECONDS, throwable);
  }

  void trackCallWithError(long duration, @Nonnull TimeUnit timeUnit, Throwable throwable);

  long getCurrentStartCountTotal();

  long getCurrentCallCountTotal();

  long getCurrentErrorCountTotal();

  int getCurrentConcurrency();

  @Nonnull
  @CheckReturnValue
  double[] getAverageConcurrency();

  @Nonnull
  @CheckReturnValue
  int[] getMaxConcurrency();

  @Nonnull
  @CheckReturnValue
  int[] getStartFrequency();

  @Nonnull
  @CheckReturnValue
  long[] getStartCount();

  @Nonnull
  @CheckReturnValue
  int[] getErrorFrequency();

  @Nonnull
  @CheckReturnValue
  long[] getErrorCount();

  @Nonnull
  @CheckReturnValue
  CallStats getCallStats();

  void reset();

  long getLastResetTime();

  long getTimeSinceLastStartCall();

  interface CallStats {
    long getCallCountTotal();

    long getCallStartCountTotal();

    long getErrorCountTotal();

    int getConcurrency();

    double getAverageConcurrency1min();

    double getAverageConcurrency5min();

    double getAverageConcurrency15min();

    int getMaxConcurrency1min();

    int getMaxConcurrency5min();

    int getMaxConcurrency15min();

    int getStartFrequency1min();

    int getStartFrequency5min();

    int getStartFrequency15min();

    int getErrorFrequency1min();

    int getErrorFrequency5min();

    int getErrorFrequency15min();

    long getOutstandingStartTimeAvg();

    int getOutstandingCount();

    /**
     * Returns the call time stats in nanoseconds.
     * @return {@linkplain LongStats} object.
     */
    LongStats getCallTimeStats();
  }

  static CallTracker create() {
    return new CallTrackerImpl();
  }

  static CallTracker nullTracker() {
    return NullCallTracker.INSTANCE;
  }
}
