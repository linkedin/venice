package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongGauge;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;
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
    this(
        metricEntity,
        otelRepository,
        baseDimensionsMap,
        registerTehutiSensorFn,
        tehutiMetricNameEnum,
        tehutiMetricStats,
        null,
        null);
  }

  public MetricEntityState(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      LongSupplier asyncCallback,
      Attributes asyncAttributes) {
    this.metricEntity = metricEntity;
    this.emitOpenTelemetryMetrics = otelRepository != null && otelRepository.emitOpenTelemetryMetrics();
    this.otelRepository = otelRepository;
    this.baseDimensionsMap = baseDimensionsMap;
    createMetric(tehutiMetricNameEnum, tehutiMetricStats, registerTehutiSensorFn, asyncCallback, asyncAttributes);
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

  /**
   * Validates
   * 1. whether an async callback is provided for an async metric
   * 2. only when tehutiMetricStats contains AsyncGauge, the metric type should be ASYNC_GAUGE and tehutiMetricStats
   *    should contain only one stat.
   *
   * @param tehutiMetricStats the tehuti metrics stats for the given metric entity.
   * @param asyncCallback the async callback function to be used for async metrics.
   */
  private void validateMetric(List<MeasurableStat> tehutiMetricStats, LongSupplier asyncCallback) {
    if (asyncCallback != null && !metricEntity.getMetricType().isAsyncMetric()) {
      throw new IllegalArgumentException(
          "Async callback is provided, but the metric type is not async for metric: " + metricEntity.getMetricName());
    } else if (asyncCallback == null && metricEntity.getMetricType().isAsyncMetric()) {
      throw new IllegalArgumentException(
          "Async callback is not provided, but the metric type is async for metric: " + metricEntity.getMetricName());
    }

    // ASYNC_GAUGE specific: If both tehuti and Otel are present, validate if all are nothing is async
    if (tehutiMetricStats == null || tehutiMetricStats.isEmpty()) {
      return;
    }
    // if tehutiMetricStats has AsyncGauge() then the metric type should be ASYNC_GAUGE
    if (tehutiMetricStats.stream().anyMatch(stat -> stat instanceof AsyncGauge)) {
      if (tehutiMetricStats.size() > 1) {
        throw new IllegalArgumentException(
            "Tehuti metric stats contains AsyncGauge, but it should be the only stat for metric: "
                + metricEntity.getMetricName());
      }
      if (metricEntity.getMetricType() != MetricType.ASYNC_GAUGE) {
        throw new IllegalArgumentException(
            "Tehuti metric stats contains AsyncGauge, but the otel metric type is not ASYNC_GAUGE for metric: "
                + metricEntity.getMetricName());
      }
    } else if (metricEntity.getMetricType() == MetricType.ASYNC_GAUGE) {
      throw new IllegalArgumentException(
          "Tehuti metric stats does not contain AsyncGauge, but the otel metric type is ASYNC_GAUGE for metric: "
              + metricEntity.getMetricName());
    }
  }

  public void createMetric(
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      LongSupplier asyncCallback,
      Attributes asyncAttributes) {
    validateMetric(tehutiMetricStats, asyncCallback);
    if (emitOpenTelemetryMetrics()) {
      setOtelMetric(otelRepository.createInstrument(this.metricEntity, asyncCallback, asyncAttributes));
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
      if (metricType.isAsyncMetric()) {
        // Async gauge metrics should be updated by the callback function.
        return;
      }
      switch (metricType) {
        case HISTOGRAM:
        case MIN_MAX_COUNT_SUM_AGGREGATIONS:
          ((DoubleHistogram) otelMetric).record(value, attributes);
          break;
        case COUNTER:
          ((LongCounter) otelMetric).add((long) value, attributes);
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
    if (metricEntity.getMetricType().isAsyncMetric()) {
      // Async gauge metrics should be updated only by the callback function.
      if (otelRepository != null) {
        otelRepository.recordFailureMetric(
            metricEntity,
            "Async gauge metrics should not call record() for metric: " + metricEntity.getMetricName());
      }
      return;
    }
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

    // validate the input dimensions and compare against the required dimensions: baseDimensionsMaps might
    // not be populated if OTel is not enabled , so checking the below only when OTel is enabled
    if (emitOpenTelemetryMetrics()) {
      if (baseAttributes != null) {
        // check 2:
        // If baseAttributes has all the dimensions in baseDimensionsMap
        if (baseAttributes.size() != baseDimensionsMap.size()) {
          throw new IllegalArgumentException(
              "baseAttributes: " + baseAttributes.asMap().keySet() + " and baseDimensionsMap: "
                  + baseDimensionsMap.keySet() + " should have the same size and values");
        }
        Map<AttributeKey<?>, Object> baseAttributesAsMap = baseAttributes.asMap();
        for (Map.Entry<VeniceMetricsDimensions, String> entry: baseDimensionsMap.entrySet()) {
          AttributeKey<String> key = AttributeKey.stringKey(otelRepository.getDimensionName(entry.getKey()));
          if (!baseAttributesAsMap.containsKey(key)
              || !Objects.equals(baseAttributesAsMap.get(key), entry.getValue())) {
            throw new IllegalArgumentException(
                "baseAttributes: " + baseAttributes.asMap()
                    + " should contain all the keys and same values as in baseDimensionsMap: " + baseDimensionsMap);
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
      } else {
        if (currentDimensions.size() != metricEntity.getDimensionsList().size()) {
          throw new IllegalArgumentException(
              "currentDimensions " + currentDimensions + " doesn't match with the required dimensions "
                  + metricEntity.getDimensionsList() + " for metric: " + metricEntity.getMetricName());
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

      // check 5:
      // If the baseDimensionsMap has all non-null values
      if (baseDimensionsMap != null) {
        for (Map.Entry<VeniceMetricsDimensions, String> entry: baseDimensionsMap.entrySet()) {
          if (entry.getValue() == null || entry.getValue().isEmpty()) {
            throw new IllegalArgumentException(
                "baseDimensionsMap " + baseDimensionsMap.keySet() + " contains a null or empty value for dimension "
                    + entry.getKey() + " for metric: " + metricEntity.getMetricName());
          }
        }
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

  Attributes createAttributes(Map<VeniceMetricsDimensions, String> dimensions) {
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

  Map<VeniceMetricsDimensions, String> getBaseDimensionsMap() {
    return baseDimensionsMap;
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
