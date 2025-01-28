package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.Sensor;
import io.tehuti.utils.RedundantLogFilter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Operational state of a metric. It holds <br>
 * 1. {@link MetricEntity}
 * 2. 1 Otel Instrument and
 * 3. multiple tehuti Sensors for this Otel Metric
 */
public class MetricEntityState {
  private static final Logger LOGGER = LogManager.getLogger(MetricEntityState.class);
  private static final RedundantLogFilter REDUNDANT_LOG_FILTER = RedundantLogFilter.getRedundantLogFilter();
  private final MetricEntity metricEntity;
  /** Otel metric */
  private Object otelMetric = null;
  /** Map of tehuti names and sensors: 1 Otel metric can cover multiple Tehuti sensors */
  private Map<TehutiMetricNameEnum, Sensor> tehutiSensors = null;

  public MetricEntityState(MetricEntity metricEntity, VeniceOpenTelemetryMetricsRepository otelRepository) {
    this(metricEntity, otelRepository, null, Collections.EMPTY_MAP);
  }

  public MetricEntityState(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensor,
      Map<TehutiMetricNameEnum, List<MeasurableStat>> tehutiMetricInput) {
    this.metricEntity = metricEntity;
    createMetric(otelRepository, tehutiMetricInput, registerTehutiSensor);
  }

  public void setOtelMetric(Object otelMetric) {
    this.otelMetric = otelMetric;
  }

  /**
   * Add Tehuti {@link Sensor} to tehutiSensors map and throw exception if sensor with same name already exists
   */
  public void addTehutiSensors(TehutiMetricNameEnum name, Sensor tehutiSensor) {
    if (tehutiSensors == null) {
      tehutiSensors = new HashMap<>();
    }
    if (tehutiSensors.put(name, tehutiSensor) != null) {
      throw new IllegalArgumentException("Sensor with name '" + name + "' already exists.");
    }
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
      Map<TehutiMetricNameEnum, List<MeasurableStat>> tehutiMetricInput,
      TehutiSensorRegistrationFunction registerTehutiSensor) {
    // Otel metric: otelRepository will be null if otel is not enabled
    if (otelRepository != null) {
      setOtelMetric(otelRepository.createInstrument(this.metricEntity));
    }
    // tehuti metric
    for (Map.Entry<TehutiMetricNameEnum, List<MeasurableStat>> entry: tehutiMetricInput.entrySet()) {
      addTehutiSensors(
          entry.getKey(),
          registerTehutiSensor
              .register(entry.getKey().getMetricName(), entry.getValue().toArray(new MeasurableStat[0])));
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

  void recordTehutiMetric(TehutiMetricNameEnum tehutiMetricNameEnum, double value) {
    if (tehutiSensors != null) {
      Sensor sensor = tehutiSensors.get(tehutiMetricNameEnum);
      if (sensor != null) {
        sensor.record(value);
      } else {
        // Log using Redundant log filters to catch any bad tehutiMetricNameEnum is passed in
        String errorLog = "Tehuti Sensor with name '" + tehutiMetricNameEnum + "' not found.";
        if (!REDUNDANT_LOG_FILTER.isRedundantLog(errorLog)) {
          LOGGER.error(errorLog);
        }
      }
    }
  }

  public void record(long value, Attributes otelDimensions) {
    recordOtelMetric(value, otelDimensions);
  }

  public void record(double value, Attributes otelDimensions) {
    recordOtelMetric(value, otelDimensions);
  }

  public void record(TehutiMetricNameEnum tehutiMetricNameEnum, long value, Attributes otelDimensions) {
    recordOtelMetric(value, otelDimensions);
    recordTehutiMetric(tehutiMetricNameEnum, value);
  }

  public void record(TehutiMetricNameEnum tehutiMetricNameEnum, double value, Attributes otelDimensions) {
    recordOtelMetric(value, otelDimensions);
    recordTehutiMetric(tehutiMetricNameEnum, value);
  }

  /** used only for testing */
  Map<TehutiMetricNameEnum, Sensor> getTehutiSensors() {
    return tehutiSensors;
  }

  /** used only for testing */
  static RedundantLogFilter getRedundantLogFilter() {
    return REDUNDANT_LOG_FILTER;
  }
}
