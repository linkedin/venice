package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MeasurableStat;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * This version of {@link MetricEntityState} is used when the metric entity has no dynamic dimensions.
 * The base {@link Attributes} that are common for all invocation of this instance are passed in the
 * constructor and used during every record() call.
 */
public class MetricEntityStateBase extends MetricEntityState {
  private final MetricAttributesData metricAttributesData;

  /** should not be called directly, call {@link #create} instead */
  private MetricEntityStateBase(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Attributes baseAttributes) {
    this(metricEntity, otelRepository, null, null, Collections.EMPTY_LIST, baseDimensionsMap, baseAttributes);
  }

  /** should not be called directly, call {@link #create} instead */
  private MetricEntityStateBase(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Attributes baseAttributes) {
    super(
        metricEntity,
        otelRepository,
        baseDimensionsMap,
        registerTehutiSensorFn,
        tehutiMetricNameEnum,
        tehutiMetricStats);
    validateRequiredDimensions(metricEntity, baseAttributes, baseDimensionsMap);
    // directly using the Attributes as multiple MetricEntityState can reuse the same base attributes object.
    // If we want to fully abstract the Attribute creation inside these classes, we can create it here instead.
    if (emitOpenTelemetryMetrics()) {
      if (baseAttributes == null) {
        throw new IllegalArgumentException(
            "Base attributes cannot be null for MetricEntityStateBase for metric: " + metricEntity.getMetricName());
      }
      this.metricAttributesData = new MetricAttributesData(baseAttributes, isObservableCounter());
      registerObservableCounterIfNeeded();
    } else {
      this.metricAttributesData = null;
    }
  }

  /** Factory method to keep the API consistent with other subclasses like {@link MetricEntityStateOneEnum} */
  public static MetricEntityStateBase create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Attributes baseAttributes) {
    return new MetricEntityStateBase(metricEntity, otelRepository, baseDimensionsMap, baseAttributes);
  }

  /** Overloaded Factory method for constructor with Tehuti parameters */
  public static MetricEntityStateBase create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Attributes baseAttributes) {
    return new MetricEntityStateBase(
        metricEntity,
        otelRepository,
        registerTehutiSensorFn,
        tehutiMetricNameEnum,
        tehutiMetricStats,
        baseDimensionsMap,
        baseAttributes);
  }

  public void record(double value) {
    super.record(value, metricAttributesData);
  }

  public void record(long value) {
    super.record(value, metricAttributesData);
  }

  @Override
  protected Iterable<MetricAttributesData> getAllMetricAttributesData() {
    if (metricAttributesData == null) {
      return null;
    }
    return Collections.singletonList(metricAttributesData);
  }

  /** visibility for testing */
  public Attributes getAttributes() {
    return metricAttributesData != null ? metricAttributesData.getAttributes() : null;
  }
}
