package com.linkedin.venice.utils;

import io.tehuti.utils.Time;


public class LatencyUtils {
  /**
   * @param startTimeInNS input start time should use nanosecond unit
   * @return latency in millisecond
   */
  public static double getLatencyInMS(long startTimeInNS) {
    return convertLatencyFromNSToMS(System.nanoTime() - startTimeInNS);
  }

  public static double convertLatencyFromNSToMS(long latencyInNS) {
    return ((double) latencyInNS) / Time.NS_PER_MS;
  }

  /**
   * @param startTimeInMs input start time should use millisecond unit
   * @return elapsed time in millisecond
   */
  public static long getElapsedTimeInMs(long startTimeInMs) {
    return System.currentTimeMillis() - startTimeInMs;
  }
}
