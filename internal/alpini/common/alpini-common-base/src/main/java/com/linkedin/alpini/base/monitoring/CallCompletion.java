package com.linkedin.alpini.base.monitoring;

import com.linkedin.alpini.base.misc.Time;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface CallCompletion extends AutoCloseable {
  default void close() {
    close(Time.nanoTime());
  }

  default void closeWithError() {
    closeWithError(Time.nanoTime());
  }

  default void closeWithError(@Nonnull Throwable error) {
    closeWithError(Time.nanoTime(), error);
  }

  default <T> void closeCompletion(T value, Throwable error) {
    closeCompletion(Time.nanoTime(), value, error);
  }

  void close(@Nonnegative long endTimeNanos);

  default void closeWithError(@Nonnegative long endTimeNanos) {
    closeWithError(endTimeNanos, CallTracker.GENERIC_EXCEPTION);
  }

  void closeWithError(@Nonnegative long endTimeNanos, @Nonnull Throwable error);

  default <T> void closeCompletion(@Nonnegative long endTimeNanos, T value, Throwable error) {
    if (error == null) {
      close(endTimeNanos);
    } else {
      closeWithError(endTimeNanos, error);
    }
  }

  static CallCompletion combine(CallCompletion... completions) {
    CallCompletion[] array = completions.clone();
    return new CallCompletion() {
      @Override
      public void close(long endTimeNanos) {
        for (CallCompletion cc: array) {
          if (cc != null) {
            cc.close(endTimeNanos);
          }
        }
      }

      @Override
      public void closeWithError(long endTimeNanos, @Nonnull Throwable error) {
        for (CallCompletion cc: array) {
          if (cc != null) {
            cc.closeWithError(endTimeNanos, error);
          }
        }
      }
    };
  }
}
