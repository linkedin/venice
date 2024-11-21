package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
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
  private Object otelMetric = null;

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

  public void setOtelMetric(Object otelMetric) {
    this.otelMetric = otelMetric;
  }

  public Object getOtelMetric() {
    return otelMetric;
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

  /**
   * create the metric
   */
  public void createMetric(VeniceOpenTelemetryMetricsRepository otelRepository) {
    otelRepository.createInstrument(this);
  }

  /**
   * Record otel metrics
   */
  private void recordOtelMetric(double value, Attributes otelDimensions) {
    if (otelMetric != null) {
      switch (metricType) {
        case HISTOGRAM:
        case HISTOGRAM_WITHOUT_BUCKETS:
          ((DoubleHistogram) otelMetric).record(value, otelDimensions);
          break;
        case COUNTER:
          ((LongCounter) otelMetric).add((long) value, otelDimensions);
          break;

        default:
          throw new IllegalArgumentException("Unsupported metric type: " + metricType);
      }
    }
  }

  public void record(long value, Attributes otelDimensions) {
    recordOtelMetric(value, otelDimensions);
  }

  public void record(double value, Attributes otelDimensions) {
    recordOtelMetric(value, otelDimensions);
  }
}
