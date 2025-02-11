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
 * Operational state of a metric. It holds: <br>
 * 1. A {@link MetricEntity} <br>
 * 2. One OpenTelemetry (Otel) Instrument <br>
 * 3. Zero or one (out of zero for new metrics or more for existing metrics) Tehuti sensors for this Otel Metric. <br>
 *
 * One Otel instrument can cover multiple Tehuti sensors through the use of dimensions. Ideally, this class should represent a one-to-many
 * mapping between an Otel instrument and Tehuti sensors. However, to simplify lookup during runtime, this class holds one Otel instrument
 * and one Tehuti sensor. If an Otel instrument corresponds to multiple Tehuti sensors, there will be multiple {@link MetricEntityState}
 * objects, each containing the same Otel instrument but different Tehuti sensors.
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
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats) {
    this.metricEntity = metricEntity;
    createMetric(otelRepository, tehutiMetricNameEnum, tehutiMetricStats, registerTehutiSensorFn);
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
      TehutiSensorRegistrationFunction registerTehutiSensorFn) {
    // Otel metric: otelRepository will be null if otel is not enabled
    if (otelRepository != null) {
      setOtelMetric(otelRepository.createInstrument(this.metricEntity));
    }
    // tehuti metric
    if (tehutiMetricStats != null && !tehutiMetricStats.isEmpty()) {
      setTehutiSensor(
          registerTehutiSensorFn
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
