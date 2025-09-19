package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongGauge;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.tehuti.metrics.MeasurableStat;
import java.util.List;
import java.util.Map;


/**
 * Abstract operational state of a non-async metric extended on top of {@link AsyncMetricEntityState}
 * to provide common functionality for non-async metrics like record() which is not supported for
 * async metrics.
 *
 * This abstract class should be extended by different MetricEntityStates like {@link MetricEntityStateBase} to
 * pre-create/cache the {@link Attributes} for different number/type of dimensions. check out the
 * classes extending this for more details. <br>
 */
public abstract class MetricEntityState extends AsyncMetricEntityState {
  public MetricEntityState(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats) {
    super(
        metricEntity,
        otelRepository,
        baseDimensionsMap,
        registerTehutiSensorFn,
        tehutiMetricNameEnum,
        tehutiMetricStats,
        null,
        null);
  }

  /**
   * Record otel metrics
   */
  public void recordOtelMetric(double value, Attributes attributes) {
    if (otelMetric != null) {
      MetricType metricType = this.metricEntity.getMetricType();
      switch (metricType) {
        case HISTOGRAM:
        case MIN_MAX_COUNT_SUM_AGGREGATIONS:
          ((DoubleHistogram) otelMetric).record(value, attributes);
          break;
        case COUNTER:
          ((LongCounter) otelMetric).add((long) value, attributes);
          break;
        case UP_DOWN_COUNTER:
          ((LongUpDownCounter) otelMetric).add((long) value, attributes);
          break;
        case GAUGE:
          ((LongGauge) otelMetric).set((long) value, attributes);
          break;
        default:
          throw new IllegalArgumentException("Unsupported metric type: " + metricType);
      }
    }
  }

  void recordTehutiMetric(double value) {
    if (tehutiSensor != null) {
      tehutiSensor.record(value);
    }
  }

  final void record(long value, Attributes attributes) {
    record(Double.valueOf(value), attributes);
  }

  final void record(double value, Attributes attributes) {
    recordOtelMetric(value, attributes);
    recordTehutiMetric(value);
  }
}
