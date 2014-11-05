package com.linkedin.venice.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * Helper functions
 */
public class Utils {
  /**
   * Print an error and exit with error code 1
   *
   * @param message The error to print
   */
  public static void croak(String message) {
    System.err.println(message);
    System.exit(1);
  }

  /**
   * Print an error and exit with the given error code
   *
   * @param message The error to print
   * @param errorCode The error code to exit with
   */
  public static void croak(String message, int errorCode) {
    System.err.println(message);
    System.exit(errorCode);
  }

  /**
   * A reversed copy of the given list
   *
   * @param <T> The type of the items in the list
   * @param l The list to reverse
   * @return The list, reversed
   */
  public static <T> List<T> reversed(List<T> l) {
    List<T> copy = new ArrayList<T>(l);
    Collections.reverse(copy);
    return copy;
  }

  /**
   * Throw an IllegalArgumentException if the argument is null, otherwise just
   * return the argument.
   *
   * @param t The thing to check for nullness.
   * @param message The message to put in the exception if it is null
   * @param <T> The type of the thing
   * @return t
   */
  public static <T> T notNull(T t, String message) {
    if (t == null) {
      throw new IllegalArgumentException(message);
    }
    return t;
  }

  /**
   * Throw an IllegalArgumentException if the argument is null, otherwise just
   * return the argument.
   *
   * Useful for assignment as in this.thing = Utils.notNull(thing);
   *
   * @param t  The thing to check for nullness.
   * @param <T>  The type of the thing
   * @return t
   */
  public static <T> T notNull(T t) {
    if (t == null) {
      throw new IllegalArgumentException("This object MUST be non-null.");
    }
    return t;
  }
}
