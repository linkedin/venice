package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.Sensor;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


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
public abstract class MetricEntityState {
  static final Logger LOGGER = LogManager.getLogger(MetricEntityState.class);
  private final boolean emitOpenTelemetryMetrics;
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
    this.emitOpenTelemetryMetrics = (otelRepository != null) && otelRepository.emitOpenTelemetryMetrics();
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
    if (emitOpenTelemetryMetrics) {
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
  void recordOtelMetric(double value, Attributes attributes) {
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

  protected final void record(long value, Attributes attributes) {
    recordOtelMetric(value, attributes);
    recordTehutiMetric(value);
  }

  protected final void record(double value, Attributes attributes) {
    recordOtelMetric(value, attributes);
    recordTehutiMetric(value);
  }

  /**
   * Validate that the {@link MetricEntityState} has all required dimensions passed in as defined in
   * {@link MetricEntity#getDimensionsList}
   */
  void validateRequiredDimensions(
      MetricEntity metricEntity,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<?>... enumTypes) {
    Set<VeniceMetricsDimensions> currentDimensions = new HashSet<>();
    for (Class<?> enumType: enumTypes) {
      if (enumType != null && enumType.isEnum() && VeniceDimensionInterface.class.isAssignableFrom(enumType)) {
        try {
          VeniceDimensionInterface[] enumConstants = (VeniceDimensionInterface[]) enumType.getEnumConstants();
          if (enumConstants.length > 0) {
            currentDimensions.add(enumConstants[0].getDimensionName());
          }
        } catch (ClassCastException e) {
          // Handle potential ClassCastException if enumType is not the expected type.
          throw new IllegalArgumentException(
              "Provided class is not a valid enum of type VeniceDimensionInterface: " + enumType.getName(),
              e);
        }
      }
    }

    // copy all baseDimensionsMap into currentDimensions
    for (Map.Entry<VeniceMetricsDimensions, String> entry: baseDimensionsMap.entrySet()) {
      currentDimensions.add(entry.getKey());
    }

    Set<VeniceMetricsDimensions> requiredDimensions = metricEntity.getDimensionsList();

    if (!requiredDimensions.containsAll(currentDimensions)) {
      currentDimensions.removeAll(requiredDimensions); // find the missing dimensions
      throw new IllegalArgumentException(
          "MetricEntity dimensions are missing some or all required dimensions: " + currentDimensions);
    }
  }

  boolean emitOpenTelemetryMetrics() {
    return emitOpenTelemetryMetrics;
  }

  public MetricEntity getMetricEntity() {
    return metricEntity;
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
