package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Percentiles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Holds {@link MetricEntity} and the operation state for the metric
 */
public class MetricEntityState {
  private MetricEntity metricEntity;
  // otel metric
  private Object otelMetric = null;
  // Map of tehuti names and sensors: 1 otel metric can cover multiple tehuti sensors
  private Map<String, Sensor> tehutiSensors = null;

  public MetricEntityState(MetricEntity metricEntity, VeniceOpenTelemetryMetricsRepository otelRepository) {
    this.metricEntity = metricEntity;
    setOtelMetric(otelRepository.createInstrument(this.metricEntity));
  }

  public MetricEntityState(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensor,
      Map<String, List<MeasurableStat>> tehutiMetricInput) {
    this.metricEntity = metricEntity;
    createMetric(otelRepository, tehutiMetricInput, registerTehutiSensor);
  }

  // setters
  public void setMetricEntity(MetricEntity metricEntity) {
    this.metricEntity = metricEntity;
  }

  public void setOtelMetric(Object otelMetric) {
    this.otelMetric = otelMetric;
  }

  public void setTehutiSensors(Map<String, Sensor> tehutiSensors) {
    this.tehutiSensors = tehutiSensors;
  }

  /**
   * Add tehuti sensor to tehutiSensors map and throw exception if sensor with same name already exists
   */
  public void addTehutiSensors(String name, Sensor tehutiSensor) {
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
      Map<String, List<MeasurableStat>> tehutiMetricInput,
      TehutiSensorRegistrationFunction registerTehutiSensor) {
    // Otel metric
    if (otelRepository != null) {
      setOtelMetric(otelRepository.createInstrument(this.metricEntity));
    }
    // tehuti metric
    // loop through tehutiMetricInput and call registerTehutiSensor for each String, List<MeasurableStat> pair
    for (Map.Entry<String, List<MeasurableStat>> entry: tehutiMetricInput.entrySet()) {
      if (entry.getValue().contains(Percentiles.class)) {
        addTehutiSensors(
            entry.getKey(),
            registerTehutiSensor.register(entry.getKey(), entry.getValue().toArray(new MeasurableStat[0])));

      } else {
        addTehutiSensors(
            entry.getKey(),
            registerTehutiSensor.register(entry.getKey(), entry.getValue().toArray(new MeasurableStat[0])));
      }
    }
  }

  /**
   * Record otel metrics
   */
  private void recordOtelMetric(double value, Attributes otelDimensions) {
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

  private void recordTehutiMetric(String tehutiMetricName, double value) {
    if (tehutiSensors != null) {
      Sensor sensor = tehutiSensors.get(tehutiMetricName);
      if (sensor != null) {
        sensor.record(value);
      }
    }
  }

  public void record(String tehutiMetricName, long value, Attributes otelDimensions) {
    recordOtelMetric(value, otelDimensions);
    recordTehutiMetric(tehutiMetricName, value);
  }

  public void record(String tehutiMetricName, double value, Attributes otelDimensions) {
    recordOtelMetric(value, otelDimensions);
    recordTehutiMetric(tehutiMetricName, value);
  }
}
