package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MeasurableStat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * This version of {@link MetricEntityState} is used when the metric entity has no dynamic dimensions.
 * The base {@link Attributes} that are common for all invocation of this instance are passed in the
 * constructor and used during every record() call.
 */
public class MetricEntityStateBase extends MetricEntityState {
  private final Attributes attributes;

  public MetricEntityStateBase(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      @Nonnull Attributes baseAttributes) {
    this(metricEntity, otelRepository, null, null, Collections.EMPTY_LIST, baseDimensionsMap, baseAttributes);
  }

  public MetricEntityStateBase(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      @Nonnull Attributes baseAttributes) {
    super(metricEntity, otelRepository, registerTehutiSensorFn, tehutiMetricNameEnum, tehutiMetricStats);
    validateRequiredDimensions(metricEntity, baseDimensionsMap);
    // directly passing in the Attributes as multiple MetricEntityState can reuse the same base attributes object.
    // If we want to fully abstract the Attribute creation inside these classes, we can create it here instead
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
