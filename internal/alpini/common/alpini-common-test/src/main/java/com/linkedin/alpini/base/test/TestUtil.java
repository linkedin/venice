package com.linkedin.alpini.base.test;

import com.linkedin.alpini.base.misc.Time;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public enum TestUtil {
  SINGLETON; // Effective Java, Item 3 - Enum singleton

  public static void waitFor(BooleanSupplier test, long duration, TimeUnit unit) {
    long timeout = Time.nanoTime() + unit.toNanos(duration);
    do {
      if (test.getAsBoolean()) {
        break;
      }
      Thread.yield();
    } while (Time.nanoTime() < timeout);
  }
}
