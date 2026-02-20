package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.commons.lang.Validate;


/**
 * Metric entity class to define a metric with all its properties
 */
public class MetricEntity {
  /**
   * The name of the metric: Only one instrument can be registered with this name in the
   * {@link com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository}
   */
  private final String metricName;
  /**
   * The type of the metric: Counter, Histogram, etc.
   */
  private final MetricType metricType;
  /**
   * The unit of the metric: MILLISECOND, NUMBER, etc.
   */
  private final MetricUnit unit;
  /**
   * The description of the metric.
   */
  private final String description;
  /**
   * The custom metric prefix: This is used to override the default metric prefix for some metrics
   * like {@link com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository.CommonMetricsEntity}
   */
  private final String customMetricPrefix;
  /**
   * List of dimensions that this metric is associated with. This is currently used to validate
   * whether the Attributes object hold all these dimensions while creating the Attributes.
   * Check {@link MetricEntityState#validateRequiredDimensions} for more details.
   */
  private final Set<VeniceMetricsDimensions> dimensionsList;

  public MetricEntity(
      @Nonnull String metricName,
      @Nonnull MetricType metricType,
      @Nonnull MetricUnit unit,
      @Nonnull String description,
      @Nonnull Set<VeniceMetricsDimensions> dimensionsList) {
    Validate.notEmpty(metricName, "Metric name cannot be empty");
    Validate.notNull(metricType, "Metric type cannot be null");
    Validate.notNull(unit, "Metric unit cannot be null");
    Validate.notEmpty(description, "Metric description cannot be empty");
    Validate.notEmpty(dimensionsList, "Dimensions list cannot be empty");
    this.metricName = metricName;
    this.metricType = metricType;
    this.unit = unit;
    this.description = description;
    this.dimensionsList = dimensionsList;
    this.customMetricPrefix = null;
  }

  @Nonnull
  public String getMetricName() {
    return metricName;
  }

  @Nonnull
  public MetricType getMetricType() {
    return metricType;
  }

  @Nonnull
  public MetricUnit getUnit() {
    return unit;
  }

  @Nonnull
  public String getDescription() {
    return description;
  }

  @Nonnull
  public Set<VeniceMetricsDimensions> getDimensionsList() {
    return dimensionsList;
  }

  public String getCustomMetricPrefix() {
    return customMetricPrefix;
  }

  /**
   * Factory method for metrics without dimensions. These metrics should only be used
   * with {@link MetricEntityStateBase}.
   */
  public static MetricEntity createWithNoDimensions(
      @Nonnull String metricName,
      @Nonnull MetricType metricType,
      @Nonnull MetricUnit unit,
      @Nonnull String description) {
    return new MetricEntity(metricName, metricType, unit, description, (String) null);
  }

  /**
   * Factory method for metrics without dimensions that need a custom metric prefix,
   * e.g. {@link com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository.CommonMetricsEntity}.
   * These metrics should only be used with {@link MetricEntityStateBase}.
   */
  public static MetricEntity createWithNoDimensions(
      @Nonnull String metricName,
      @Nonnull MetricType metricType,
      @Nonnull MetricUnit unit,
      @Nonnull String description,
      @Nonnull String customMetricPrefix) {
    Validate.notEmpty(customMetricPrefix, "Custom metric prefix cannot be empty");
    if (customMetricPrefix.startsWith("venice.")) {
      // venice will be added automatically
      throw new IllegalArgumentException("Custom prefix should not start with venice: " + customMetricPrefix);
    }
    return new MetricEntity(metricName, metricType, unit, description, customMetricPrefix);
  }

  /** Private constructor for {@link #createWithNoDimensions} factory methods */
  private MetricEntity(
      @Nonnull String metricName,
      @Nonnull MetricType metricType,
      @Nonnull MetricUnit unit,
      @Nonnull String description,
      String customMetricPrefix) {
    Validate.notEmpty(metricName, "Metric name cannot be empty");
    Validate.notNull(metricType, "Metric type cannot be null");
    Validate.notNull(unit, "Metric unit cannot be null");
    Validate.notEmpty(description, "Metric description cannot be empty");
    this.metricName = metricName;
    this.metricType = metricType;
    this.unit = unit;
    this.description = description;
    this.dimensionsList = Collections.EMPTY_SET;
    this.customMetricPrefix = customMetricPrefix;
  }
}
