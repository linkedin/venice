package com.linkedin.venice.utils;

import javax.annotation.Nullable;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;


/**
 * Utility class representing structured logging context information, such as
 * component and region name. Used to enrich log messages with identifying context
 * via Log4j2's {@link ThreadContext}.
 */
public class LogContext {
  private static final Logger LOGGER = LogManager.getLogger(LogContext.class);
  public static final LogContext EMPTY = new LogContext(new Builder());
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
    if (StringUtils.isBlank(regionName) && StringUtils.isBlank(componentName)) {
      this.value = "";
    } else {
      this.value = String.format("%s--%s", componentName, regionName);
    }
  }

  /**
   * Updates the ThreadContext (MDC) with a logging context derived from the given region name.
   * If the region name is blank or null, no update is performed.
   *
   * @param regionName The name of the region to include in the log context.
   */
  public static void setRegionLogContext(String regionName) {
    putLogContextKeyValue(regionName);
  }

  public static void setStructuredLogContext(@Nullable LogContext logContext) {
    if (logContext != null) {
      putLogContextKeyValue(logContext.getValue());
    }
  }

  public static void setLogContext(Object logContext) {
    if (logContext instanceof String) {
      putLogContextKeyValue((String) logContext);
    } else if (logContext instanceof LogContext) {
      putLogContextKeyValue(((LogContext) logContext).getValue());
    }
  }

  private static void putLogContextKeyValue(String value) {
    try {
      if (StringUtils.isNotBlank(value)) {
        ThreadContext.put(LOG_CONTEXT_KEY, value);
      }
    } catch (Exception e) {
      LOGGER.debug("Failed to clear ThreadContext", e);
      // Ignore any exceptions that occur while setting the log context.
      // This is a best-effort operation and should not disrupt normal logging.
    }
  }

  public static void clearLogContext() {
    try {
      ThreadContext.remove(LOG_CONTEXT_KEY);
    } catch (Exception e) {
      // Ignore any exceptions that occur while clearing the log context.
      // This is a best-effort operation and should not disrupt normal logging.
      LOGGER.debug("Failed to clear ThreadContext", e);
    }
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
