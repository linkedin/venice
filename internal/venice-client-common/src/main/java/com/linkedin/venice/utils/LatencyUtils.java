package com.linkedin.venice.utils;

import io.tehuti.utils.Time;


public class LatencyUtils {
  /**
   * @param startTimeInNS input start time should use nanosecond unit
   * @return latency in millisecond
   */
  public static double getElapsedTimeFromNSToMS(long startTimeInNS) {
    return convertNSToMS(System.nanoTime() - startTimeInNS);
  }

  public static double convertNSToMS(long nanoseconds) {
    return ((double) nanoseconds) / Time.NS_PER_MS;
  }

  /**
   * @param startTimeInMs input start time should use millisecond unit
   * @return elapsed time in millisecond
   */
  public static long getElapsedTimeFromMsToMs(long startTimeInMs) {
    return System.currentTimeMillis() - startTimeInMs;
  }

  /***
   * Sleep until number of milliseconds have passed, or the operation is interrupted.  This method will swallow the
   * InterruptedException and terminate, if this is used in a loop it may become difficult to cleanly break out
   * of the loop.
   *
   * @param millis
   * @return true on success and false if sleep was interrupted
   */
  public static boolean sleep(long millis) {
    try {
      Thread.sleep(millis);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  public static long getLatencyInNS(long startTimeInNS) {
    return System.nanoTime() - startTimeInNS;
  }
}
