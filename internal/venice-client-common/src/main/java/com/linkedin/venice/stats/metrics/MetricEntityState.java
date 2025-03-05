package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.Sensor;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Abstract operational state of a metric which should be extended by different MetricEntityStates
 * to pre-create/cache the {@link Attributes} for different number/type of dimensions. check out the
 * classes extending this for more details. <br>
 *
 * This abstract class holds: <br>
 * 1. Whether otel is enabled or not <br>
 * 2. A {@link MetricEntity} <br>
 * 3. One OpenTelemetry (Otel) Instrument <br>
 * 4. Zero or one (out of zero for new metrics or more for existing metrics) Tehuti sensors for this Otel Metric. <br>
 *
 * One Otel instrument can cover multiple Tehuti sensors through the use of dimensions. Ideally, this class should represent a one-to-many
 * mapping between an Otel instrument and Tehuti sensors. However, to simplify lookup during runtime, this class holds one Otel instrument
 * and one Tehuti sensor. If an Otel instrument corresponds to multiple Tehuti sensors, there will be multiple {@link MetricEntityState}
 * objects, each containing the same Otel instrument but different Tehuti sensors.
 */
public abstract class MetricEntityState {
  static final Logger LOGGER = LogManager.getLogger(MetricEntityState.class);
  private final boolean emitOpenTelemetryMetrics;
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final MetricEntity metricEntity;
  /** Otel metric */
  private Object otelMetric = null;
  /** Respective tehuti metric */
  private Sensor tehutiSensor = null;

  public MetricEntityState(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats) {
    this.metricEntity = metricEntity;
    this.emitOpenTelemetryMetrics = (otelRepository != null) && otelRepository.emitOpenTelemetryMetrics();
    this.otelRepository = otelRepository;
    createMetric(tehutiMetricNameEnum, tehutiMetricStats, registerTehutiSensorFn);
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
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      TehutiSensorRegistrationFunction registerTehutiSensorFn) {
    if (emitOpenTelemetryMetrics()) {
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
      Attributes baseAttributes,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<?>... enumTypes) {
    Set<VeniceMetricsDimensions> currentDimensions = new HashSet<>();
    for (Class<?> enumType: enumTypes) {
      if (enumType != null && enumType.isEnum() && VeniceDimensionInterface.class.isAssignableFrom(enumType)) {
        try {
          VeniceDimensionInterface[] enumConstants = (VeniceDimensionInterface[]) enumType.getEnumConstants();
          if (enumConstants.length > 0) {
            currentDimensions.add(enumConstants[0].getDimensionName());
          } else {
            throw new IllegalArgumentException(
                "Enum type " + enumType.getName() + " has no constants for MetricEntity: "
                    + metricEntity.getMetricName());
          }
        } catch (ClassCastException e) {
          // Handle potential ClassCastException if enumType is not the expected type.
          throw new IllegalArgumentException(
              "Provided class is not a valid enum of type VeniceDimensionInterface: " + enumType.getName()
                  + " for MetricEntity: " + metricEntity.getMetricName(),
              e);
        }
      }
    }

    // validate the input dimensions and compare against the required dimensions
    if (emitOpenTelemetryMetrics()) {
      if (baseAttributes != null) {
        // check if baseAttributes has all the dimensions in baseDimensionsMap
        if (baseAttributes.size() != baseDimensionsMap.size()) {
          throw new IllegalArgumentException(
              "baseAttributes: " + baseAttributes.asMap().keySet() + " and baseDimensionsMap: "
                  + baseDimensionsMap.keySet() + " should have the same size and values");
        }
        for (Map.Entry<VeniceMetricsDimensions, String> entry: baseDimensionsMap.entrySet()) {
          AttributeKey<String> key = AttributeKey.stringKey(otelRepository.getDimensionName(entry.getKey()));
          Map<AttributeKey<?>, Object> baseAttributesAsMap = baseAttributes.asMap();
          if (!baseAttributesAsMap.containsKey(key)
              || !Objects.equals(baseAttributesAsMap.get(key), entry.getValue())) {
            throw new IllegalArgumentException(
                "baseAttributes: " + baseAttributes.asMap().keySet()
                    + " should contain all the keys in baseDimensionsMap: " + baseDimensionsMap.keySet());
          }
        }
      }

      // check if the required dimensions match with the base+current dimensions
      // copy all baseDimensionsMap into currentDimensions
      if (baseDimensionsMap != null) {
        currentDimensions.addAll(baseDimensionsMap.keySet());
      }
      Set<VeniceMetricsDimensions> requiredDimensions = metricEntity.getDimensionsList();

      if (!requiredDimensions.equals(currentDimensions)) {
        throw new IllegalArgumentException(
            "Input dimensions " + currentDimensions + " doesn't match with the required dimensions "
                + requiredDimensions + " for metric: " + metricEntity.getMetricName());
      }
    }
  }

  final boolean emitOpenTelemetryMetrics() {
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
