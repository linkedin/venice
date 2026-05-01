package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
   * like {@link com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository.CommonMetricsEntity}.
   * {@code null} means use the default prefix.
   */
  @Nullable
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
    validateCommonFields(metricName, metricType, unit, description);
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

  @Nullable
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
    return new MetricEntity(metricName, metricType, unit, description);
  }

  /** Private constructor for {@link #createWithNoDimensions}: no dimensions, no custom prefix. */
  private MetricEntity(
      @Nonnull String metricName,
      @Nonnull MetricType metricType,
      @Nonnull MetricUnit unit,
      @Nonnull String description) {
    validateCommonFields(metricName, metricType, unit, description);
    this.metricName = metricName;
    this.metricType = metricType;
    this.unit = unit;
    this.description = description;
    this.dimensionsList = Collections.emptySet();
    this.customMetricPrefix = null;
  }

  /**
   * Factory for metrics that have both dimensions AND a custom metric prefix
   * (e.g. internal infrastructure counters in
   * {@link com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository.CommonMetricsEntity}).
   */
  public static MetricEntity createWithCustomPrefix(
      @Nonnull String metricName,
      @Nonnull MetricType metricType,
      @Nonnull MetricUnit unit,
      @Nonnull String description,
      @Nonnull Set<VeniceMetricsDimensions> dimensionsList,
      @Nonnull String customMetricPrefix) {
    return new MetricEntity(metricName, metricType, unit, description, dimensionsList, customMetricPrefix);
  }

  /** Private constructor for {@link #createWithCustomPrefix}: dimensions + custom prefix. */
  private MetricEntity(
      @Nonnull String metricName,
      @Nonnull MetricType metricType,
      @Nonnull MetricUnit unit,
      @Nonnull String description,
      @Nonnull Set<VeniceMetricsDimensions> dimensionsList,
      @Nonnull String customMetricPrefix) {
    validateCommonFields(metricName, metricType, unit, description);
    validateCustomMetricPrefix(customMetricPrefix);
    Validate.notEmpty(dimensionsList, "Dimensions list cannot be empty");
    this.metricName = metricName;
    this.metricType = metricType;
    this.unit = unit;
    this.description = description;
    this.dimensionsList = dimensionsList;
    this.customMetricPrefix = customMetricPrefix;
  }

  /** Validates fields common to all constructors, including the RATIO/double-type constraint. */
  private static void validateCommonFields(
      String metricName,
      MetricType metricType,
      MetricUnit unit,
      String description) {
    Validate.notEmpty(metricName, "Metric name cannot be empty");
    Validate.notNull(metricType, "Metric type cannot be null");
    Validate.notNull(unit, "Metric unit cannot be null");
    Validate.notEmpty(description, "Metric description cannot be empty");
    if (unit == MetricUnit.RATIO && !metricType.supportsDoubleValues()) {
      throw new IllegalArgumentException(
          "MetricUnit.RATIO requires a double-capable metric type (HISTOGRAM, MIN_MAX_COUNT_SUM_AGGREGATIONS, "
              + "or ASYNC_DOUBLE_GAUGE), but got: " + metricType + " for metric: " + metricName);
    }
  }

  /**
   * Validates a custom metric prefix at construction time so the rule is unbypassable across
   * factories. Any prefix beginning with {@code venice} is rejected because the default
   * {@code venice.} prefix is auto-prepended; allowing a custom prefix that starts with
   * {@code venice} (with or without the trailing dot) would yield {@code venice.venice…} metric
   * names.
   */
  private static void validateCustomMetricPrefix(String customMetricPrefix) {
    Validate.notEmpty(customMetricPrefix, "Custom metric prefix cannot be empty");
    if (customMetricPrefix.startsWith("venice")) {
      throw new IllegalArgumentException("Custom prefix should not start with venice: " + customMetricPrefix);
    }
  }
}
