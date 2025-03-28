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
import java.util.Arrays;
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
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;
  private final MetricEntity metricEntity;

  /** Otel metric */
  private Object otelMetric = null;
  /** Respective tehuti metric */
  private Sensor tehutiSensor = null;

  public MetricEntityState(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats) {
    this.metricEntity = metricEntity;
    this.emitOpenTelemetryMetrics = otelRepository != null && otelRepository.emitOpenTelemetryMetrics();
    this.otelRepository = otelRepository;
    this.baseDimensionsMap = baseDimensionsMap;
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

  final void record(long value, Attributes attributes) {
    recordOtelMetric(value, attributes);
    recordTehutiMetric(value);
  }

  final void record(double value, Attributes attributes) {
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

    // Check 1:
    // If currentDimensions size and enumTypes are the same. If not, it could mean:
    // 1. there is a duplicate class passed in, or
    // 2. 2 different classes have the same VeniceMetricsDimensions
    if (enumTypes.length != currentDimensions.size()) {
      throw new IllegalArgumentException(
          "The enumTypes: " + Arrays.toString(enumTypes) + " has duplicate dimensions for MetricEntity: "
              + metricEntity.getMetricName());
    }

    // validate the input dimensions and compare against the required dimensions: baseDimensionsMaps might be
    // populated only if emitOpenTelemetryMetrics is true, so checking only when OTel is enabled
    if (emitOpenTelemetryMetrics()) {
      if (baseAttributes != null) {
        // check 2:
        // If baseAttributes has all the dimensions in baseDimensionsMap
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

      // check 3:
      // If the baseDimensionsMap.size() + currentDimensions.size() is equal to the required
      // dimensions. If check 4 passes and this fails, that means there is some duplicate dimensions.
      if (baseDimensionsMap != null) {
        if (baseDimensionsMap.size() + currentDimensions.size() != metricEntity.getDimensionsList().size()) {
          throw new IllegalArgumentException(
              "baseDimensionsMap " + baseDimensionsMap.keySet() + " and currentDimensions " + currentDimensions
                  + " doesn't match with the required dimensions " + metricEntity.getDimensionsList() + " for metric: "
                  + metricEntity.getMetricName());
        }
      }

      // check 4:
      // If the required dimensions match with the base+current dimensions
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

  /**
   * Validates whether the input dimensions passed is not null and throw IllegalArgumentException
   */
  void validateInputDimension(VeniceDimensionInterface dimension) {
    if (dimension == null) {
      throw new IllegalArgumentException(
          "The input Otel dimension cannot be null for metric Entity: " + getMetricEntity().getMetricName());
    }
  }

  /**
   * Create the {@link Attributes} for the given dimensions and baseDimensionsMap
   */
  Attributes createAttributes(VeniceDimensionInterface... dimensions) {
    return getOtelRepository().createAttributes(metricEntity, baseDimensionsMap, dimensions);
  }

  final boolean emitOpenTelemetryMetrics() {
    return emitOpenTelemetryMetrics;
  }

  MetricEntity getMetricEntity() {
    return metricEntity;
  }

  VeniceOpenTelemetryMetricsRepository getOtelRepository() {
    return otelRepository;
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
