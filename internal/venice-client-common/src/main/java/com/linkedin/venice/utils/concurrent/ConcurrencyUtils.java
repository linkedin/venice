package com.linkedin.venice.utils.concurrent;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BooleanSupplier;


public final class ConcurrencyUtils {
  private ConcurrencyUtils() {
  }

  public static void executeUnderConditionalLock(Runnable action, BooleanSupplier lockCondition, Object lock) {
    if (lockCondition.getAsBoolean()) {
      synchronized (lock) {
        // Check it again
        if (lockCondition.getAsBoolean()) {
          action.run();
        }
      }
    }
  }

  public static void executeUnderLock(Runnable action, Runnable orElse, BooleanSupplier lockCondition, Object lock) {
    synchronized (lock) {
      if (lockCondition.getAsBoolean()) {
        action.run();
      } else {
        orElse.run();
      }
    }
  }

  /**
   * Waits for the future and returns its result. A failure of the task is rethrown as if the task had run on this
   * thread: a {@link RuntimeException} or an {@link Error} as is, any other exception wrapped in a
   * {@link VeniceException}. If this thread is interrupted while waiting, its interrupt status is kept and a
   * {@link VeniceException} with the given message is thrown.
   */
  public static <T> T getOrRethrowCause(Future<T> future, String interruptedMessage) {
    try {
      return future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new VeniceException(interruptedMessage, e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw new VeniceException(cause);
    }
  }
}
