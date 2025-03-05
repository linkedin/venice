package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.commons.lang.Validate;


/**
 * Metric entity class to define a metric with all its properties
 */
public class MetricEntity {
  private final String metricName;
  private final MetricType metricType;
  private final MetricUnit unit;
  private final String description;
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
}
