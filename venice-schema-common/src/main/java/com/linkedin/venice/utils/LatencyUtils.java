package com.linkedin.venice.utils;

import io.tehuti.utils.Time;


public class LatencyUtils {
  public static double getLatencyInMS(long startTimeInNS) {
    return convertLatencyFromNSToMS(System.nanoTime() - startTimeInNS);
  }

  public static double convertLatencyFromNSToMS(long latencyInNS) {
    return ((double)latencyInNS) / Time.NS_PER_MS;
  }
}
