package com.linkedin.venice.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.ThreadContext;


/**
 * Utility class representing structured logging context information, such as
 * component and region name. Used to enrich log messages with identifying context
 * via Log4j2's {@link ThreadContext}.
 */
public class LogContext {
  /**
   * Key used in the logging context (e.g., ThreadContext/MDC) to tag log lines
   * with the component and region name. Helps disambiguate log output in
   * multi-cluster or multi-component test setups.
   */
  public static final String LOG_CONTEXT_KEY = "logContext";

  private final String regionName;
  private final String componentName;
  private final String value;

  private LogContext(Builder builder) {
    this.regionName = builder.regionName;
    this.componentName = builder.componentName;
    this.value = String.format("%s|%s", componentName, regionName);
  }

  /**
   * Updates the ThreadContext (MDC) with a logging context derived from the given region name.
   * If the region name is blank or null, no update is performed.
   *
   * @param regionName The name of the region to include in the log context.
   */
  public static void updateThreadContext(String regionName) {
    String loggingContext = getLoggingContext(regionName);
    if (StringUtils.isNotBlank(loggingContext)) {
      ThreadContext.put(LOG_CONTEXT_KEY, loggingContext);
    }
  }

  /**
   * Returns the region name as the logging context string, or an empty string if null.
   */
  private static String getLoggingContext(String regionName) {
    if (regionName == null) {
      return "";
    }
    return regionName;
  }

  /**
   * @return The region name stored in this LogContext.
   */
  public String getRegionName() {
    return regionName;
  }

  /**
   * @return The component name stored in this LogContext.
   */
  public String getComponentName() {
    return componentName;
  }

  /**
   * @return The value of the log context in the form {@code componentName|regionName}.
   */
  public String getValue() {
    return value;
  }

  /**
   * Returns a formatted string representation of the log context in the form:
   * {@code componentName|regionName}.
   */
  @Override
  public String toString() {
    return getValue();
  }

  /**
   * @return A new builder for constructing {@link LogContext} instances.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for {@link LogContext}.
   */
  public static class Builder {
    private String regionName;
    private String componentName;

    /**
     * Sets the region name to be included in the LogContext.
     *
     * @param regionName The name of the region.
     * @return This builder instance.
     */
    public Builder setRegionName(String regionName) {
      this.regionName = regionName;
      return this;
    }

    /**
     * Sets the component name to be included in the LogContext.
     *
     * @param componentName The name of the component.
     * @return This builder instance.
     */
    public Builder setComponentName(String componentName) {
      this.componentName = componentName;
      return this;
    }

    /**
     * Builds a new {@link LogContext} using the provided values.
     *
     * @return A new LogContext instance.
     */
    public LogContext build() {
      return new LogContext(this);
    }
  }
}
