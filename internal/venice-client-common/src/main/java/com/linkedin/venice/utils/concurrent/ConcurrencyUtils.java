package com.linkedin.venice.utils.concurrent;

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
}
