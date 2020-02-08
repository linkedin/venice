package com.linkedin.venice.router.stats;

public class StatsUtils {

  public static String convertHostnameToMetricName(String hostName) {
    return hostName.replace('.', '_');
  }
}
