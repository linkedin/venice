package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.Sensor;
import java.util.Collections;
import java.util.List;


/**
 * Operational state of a metric. It holds <br>
 * 1. {@link MetricEntity}
 * 2. 1 Otel Instrument and
 * 3. 0 or 1 (out of 0 (for new metric) or more(for existing metrics)) tehuti Sensors for this Otel Metrics. 1 Otel metric can
 *    cover multiple Tehuti sensors with the use of dimensions. Ideally this should be 1 otel instrument to n tehuti sensors map,
 *    but to keep the lookup simple during runtime, this class holds 1 Otel and 1 tehuti sensor. If an otel instrument has n
 *    tehuti sensors, there will be n {@link MetricEntityState} objects and each object will have the same otel instrument but
 *    different tehuti sensors.
 */
public class MetricEntityState {
  private final MetricEntity metricEntity;
  /** Otel metric */
  private Object otelMetric = null;
  /** Respective tehuti metric */
  private Sensor tehutiSensor = null;

  public MetricEntityState(MetricEntity metricEntity, VeniceOpenTelemetryMetricsRepository otelRepository) {
    this(metricEntity, otelRepository, null, null, Collections.EMPTY_LIST);
  }

  public MetricEntityState(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensor,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats) {
    this.metricEntity = metricEntity;
    createMetric(otelRepository, tehutiMetricNameEnum, tehutiMetricStats, registerTehutiSensor);
  }

  public void setOtelMetric(Object otelMetric) {
    this.otelMetric = otelMetric;
  }

  public void setTehutiSensor(Sensor tehutiSensor) {
    this.tehutiSensor = tehutiSensor;
  }

  /**
   * create the metrics/Sensors
   */
  @FunctionalInterface
  public interface TehutiSensorRegistrationFunction {
    Sensor register(String sensorName, MeasurableStat... stats);
  }

  public void createMetric(
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      TehutiSensorRegistrationFunction registerTehutiSensor) {
    // Otel metric: otelRepository will be null if otel is not enabled
    if (otelRepository != null) {
      setOtelMetric(otelRepository.createInstrument(this.metricEntity));
    }
    // tehuti metric
    if (tehutiMetricStats != null && !tehutiMetricStats.isEmpty()) {
      setTehutiSensor(
          registerTehutiSensor
              .register(tehutiMetricNameEnum.getMetricName(), tehutiMetricStats.toArray(new MeasurableStat[0])));
    }
  }

  /**
   * Record otel metrics
   */
  void recordOtelMetric(double value, Attributes otelDimensions) {
    if (otelMetric != null) {
      MetricType metricType = this.metricEntity.getMetricType();
      switch (metricType) {
        case HISTOGRAM:
        case MIN_MAX_COUNT_SUM_AGGREGATIONS:
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

  void recordTehutiMetric(double value) {
    if (tehutiSensor != null) {
      tehutiSensor.record(value);
    }
  }

  public void record(long value, Attributes otelDimensions) {
    recordOtelMetric(value, otelDimensions);
    recordTehutiMetric(value);
  }

  public void record(double value, Attributes otelDimensions) {
    recordOtelMetric(value, otelDimensions);
    recordTehutiMetric(value);
  }

  /** used only for testing */
  Sensor getTehutiSensor() {
    return tehutiSensor;
  }

  /** used only for testing */
  Object getOtelMetric() {
    return otelMetric;
  }
}
