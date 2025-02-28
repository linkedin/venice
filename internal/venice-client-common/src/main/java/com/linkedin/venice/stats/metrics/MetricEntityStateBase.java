package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MeasurableStat;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;


public class MetricEntityStateBase extends MetricEntityState {
  private final Attributes attributes;

  public MetricEntityStateBase(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      @Nonnull Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      @Nonnull Attributes baseAttributes) {
    super(metricEntity, otelRepository);
    validateRequiredDimensions(metricEntity, baseDimensionsMap);
    // directly passing in the Attributes as multiple MetricEntityState can reuse the same base dimensions object.
    // If we want to fully abstract the Attribute creation inside these classes, we can create it here instead
    this.attributes = baseAttributes;
  }

  public MetricEntityStateBase(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      @Nonnull Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      @Nonnull Attributes baseAttributes) {
    super(metricEntity, otelRepository, registerTehutiSensorFn, tehutiMetricNameEnum, tehutiMetricStats);
    validateRequiredDimensions(metricEntity, baseDimensionsMap);
    // directly passing in the Attributes as multiple MetricEntityState can reuse the same base dimensions
    // if we want to fully abstract the Attribute creation inside these classes, we can create it here instead
    this.attributes = baseAttributes;
  }

  public void record(long value) {
    super.record(value, attributes);
  }

  public void record(double value) {
    super.record(value, attributes);
  }

  /** visibility for testing */
  public Attributes getAttributes() {
    return attributes;
  }
}
