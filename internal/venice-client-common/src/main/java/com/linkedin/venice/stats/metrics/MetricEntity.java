package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang.Validate;


/**
 * Metric entity class to define a metric with all its properties
 */
public class MetricEntity {
  private final String metricName;
  private final MetricType metricType;
  private final MetricUnit unit;
  private final String description;
  private final Set<VeniceMetricsDimensions> dimensionsList;

  public MetricEntity(
      @Nonnull String metricName,
      @Nonnull MetricType metricType,
      @Nonnull MetricUnit unit,
      @Nonnull String description) {
    this(metricName, metricType, unit, description, null);
  }

  public MetricEntity(
      @Nonnull String metricName,
      @Nonnull MetricType metricType,
      @Nonnull MetricUnit unit,
      @Nonnull String description,
      @Nullable Set<VeniceMetricsDimensions> dimensionsList) {
    Validate.notEmpty(metricName, "Metric name cannot be null or empty");
    this.metricName = metricName;
    this.metricType = metricType;
    this.unit = unit;
    this.description = description;
    this.dimensionsList = dimensionsList;
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

  @Nullable
  public Set<VeniceMetricsDimensions> getDimensionsList() {
    return dimensionsList;
  }
}
