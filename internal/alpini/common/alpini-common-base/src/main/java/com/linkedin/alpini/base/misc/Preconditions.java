package com.linkedin.alpini.base.misc;

/**
 * Utility methods for precondition checks.
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public enum Preconditions {
  SINGLETON; // Effective Java, Item 3 - Enum singleton

  public static boolean isNullOrEmpty(String text) {
    return null == text || text.isEmpty();
  }

  public static String requireNotEmpty(String text) {
    if (isNullOrEmpty(text)) {
      throw new IllegalArgumentException();
    }
    return text;
  }

  public static String requireNotEmpty(String text, String message) {
    if (isNullOrEmpty(text)) {
      throw new IllegalArgumentException(message);
    }
    return text;
  }

  public static void checkState(boolean state) {
    if (!state) {
      throw new IllegalStateException();
    }
  }

  public static void checkState(boolean state, String message) {
    if (!state) {
      throw new IllegalStateException(message);
    }
  }

  public static int notLessThan(int value, int level, String message) {
    if (value < level) {
      throw new IllegalArgumentException(message);
    }
    return value;
  }

  public static long notLessThan(long value, long level, String message) {
    if (value < level) {
      throw new IllegalArgumentException(message);
    }
    return value;
  }

  public static int between(int value, String name, int min, int max) {
    if (value < min || value > max) {
      throw new IllegalArgumentException(name + ": " + value + " (expected: " + min + "-" + max + ")");
    }
    return value;
  }
}
