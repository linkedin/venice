package com.linkedin.venice.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.ThreadContext;


public class LogContextHelper {
  /**
   * Key used in the logging context (e.g., ThreadContext/MDC) to tag log lines
   * with the component and region name. Helps disambiguate log output in
   * multi-cluster or multi-component test setups.
   */
  public static final String LOG_CONTEXT_KEY = "logContext";

  public static void updateThreadContext(String regionName) {
    String loggingContext = getLoggingContext(regionName);
    if (StringUtils.isNotBlank(loggingContext)) {
      ThreadContext.put(LOG_CONTEXT_KEY, loggingContext);
    }
  }

  private static String getLoggingContext(String regionName) {
    if (regionName == null) {
      return "";
    }
    return regionName;
  }
}
