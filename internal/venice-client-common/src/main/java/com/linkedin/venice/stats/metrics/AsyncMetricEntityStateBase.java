package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MeasurableStat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;
import javax.annotation.Nonnull;
import org.apache.commons.lang.Validate;


/**
 * This version of {@link AsyncMetricEntityState} is used when the metric entity has no dynamic dimensions.
 * The base {@link Attributes} that are common for all invocation of this instance are passed in the
 * constructor and used during async callback recording.
 */
public class AsyncMetricEntityStateBase extends AsyncMetricEntityState {
  /** Primary constructor for LongSupplier (ASYNC_GAUGE). */
  private AsyncMetricEntityStateBase(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Attributes baseAttributes,
      LongSupplier asyncCallback) {
    super(
        metricEntity,
        otelRepository,
        baseDimensionsMap,
        registerTehutiSensorFn,
        tehutiMetricNameEnum,
        tehutiMetricStats,
        asyncCallback,
        baseAttributes);
    validateBaseAttributes(metricEntity, baseAttributes, baseDimensionsMap);
  }

  /** Primary constructor for DoubleSupplier (ASYNC_DOUBLE_GAUGE). */
  private AsyncMetricEntityStateBase(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Attributes baseAttributes,
      DoubleSupplier asyncDoubleCallback) {
    super(
        metricEntity,
        otelRepository,
        baseDimensionsMap,
        registerTehutiSensorFn,
        tehutiMetricNameEnum,
        tehutiMetricStats,
        asyncDoubleCallback,
        baseAttributes);
    validateBaseAttributes(metricEntity, baseAttributes, baseDimensionsMap);
  }

  private void validateBaseAttributes(
      MetricEntity metricEntity,
      Attributes baseAttributes,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap) {
    validateRequiredDimensions(metricEntity, baseAttributes, baseDimensionsMap);
    if (emitOpenTelemetryMetrics()) {
      Validate.notNull(
          baseAttributes,
          "Base attributes cannot be null for MetricEntityStateBase for metric: " + metricEntity.getMetricName());
    }
  }

  // --- LongSupplier factory methods (for ASYNC_GAUGE) ---

  /** Factory method for OTel-only ASYNC_GAUGE with LongSupplier callback */
  public static AsyncMetricEntityStateBase create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Attributes baseAttributes,
      @Nonnull LongSupplier asyncCallback) {
    return new AsyncMetricEntityStateBase(
        metricEntity,
        otelRepository,
        null,
        null,
        Collections.emptyList(),
        baseDimensionsMap,
        baseAttributes,
        asyncCallback);
  }

  /** Factory method for joint Tehuti+OTel ASYNC_GAUGE with LongSupplier callback */
  public static AsyncMetricEntityStateBase create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Attributes baseAttributes,
      @Nonnull LongSupplier asyncCallback) {
    return new AsyncMetricEntityStateBase(
        metricEntity,
        otelRepository,
        registerTehutiSensorFn,
        tehutiMetricNameEnum,
        tehutiMetricStats,
        baseDimensionsMap,
        baseAttributes,
        asyncCallback);
  }

  // --- DoubleSupplier factory methods (for ASYNC_DOUBLE_GAUGE) ---

  /** Factory method for OTel-only ASYNC_DOUBLE_GAUGE with DoubleSupplier callback */
  public static AsyncMetricEntityStateBase create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Attributes baseAttributes,
      @Nonnull DoubleSupplier asyncDoubleCallback) {
    return new AsyncMetricEntityStateBase(
        metricEntity,
        otelRepository,
        null,
        null,
        Collections.emptyList(),
        baseDimensionsMap,
        baseAttributes,
        asyncDoubleCallback);
  }

  /** Factory method for joint Tehuti+OTel ASYNC_DOUBLE_GAUGE with DoubleSupplier callback */
  public static AsyncMetricEntityStateBase create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Attributes baseAttributes,
      @Nonnull DoubleSupplier asyncDoubleCallback) {
    return new AsyncMetricEntityStateBase(
        metricEntity,
        otelRepository,
        registerTehutiSensorFn,
        tehutiMetricNameEnum,
        tehutiMetricStats,
        baseDimensionsMap,
        baseAttributes,
        asyncDoubleCallback);
  }
}
