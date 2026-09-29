package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AsyncMetricResolvers.LiveStateResolver;
import com.linkedin.venice.stats.metrics.AsyncMetricResolvers.ValueResolver;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Abstract operational state of a metric entity. Subclassed by:
 * <ol>
 *   <li>{@link AsyncMetricEntityStateBase} — async gauge with a fixed attribute set (no dynamic
 *       dimensions).</li>
 *   <li>{@link MetricEntityState} — non-async metrics (sync counter, histogram, gauge) and
 *       observable counters. Provides the {@code record()} API.</li>
 * </ol>
 *
 * Async gauges with enum dimensions use {@link AsyncMetricEntityStateOneEnum} /
 * {@link AsyncMetricEntityStateTwoEnums} instead — these register a single multi-emit observable
 * gauge per metric entity and do not subclass this base.
 *
 * <p>This class holds:
 * <ul>
 *   <li>Whether OTel is enabled.</li>
 *   <li>The {@link MetricEntity} definition.</li>
 *   <li>At most one OTel instrument (stored in {@link #otelMetric}).</li>
 *   <li>At most one Tehuti sensor. When one OTel metric corresponds to multiple Tehuti sensors
 *       (e.g., two Tehuti sensors mapped to a single OTel metric via a dimension), there are
 *       multiple instances of this class each with the same OTel instrument handle but a
 *       different Tehuti sensor.</li>
 * </ul>
 */
public abstract class AsyncMetricEntityState implements AutoCloseable {
  private final boolean emitOpenTelemetryMetrics;
  private final boolean emitTehutiMetrics;
  protected final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;
  protected final MetricEntity metricEntity;

  /** Otel metric */
  protected Object otelMetric = null;
  /** Respective tehuti metric */
  protected Sensor tehutiSensor = null;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public AsyncMetricEntityState(
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
        false);
    validateAsyncCallback(false);
    if (emitOpenTelemetryMetrics()) {
      // Observable counters return null from createInstrument() and are registered later by
      // MetricEntityState#registerObservableCounterIfNeeded().
      setOtelMetric(otelRepository.createInstrument(this.metricEntity));
    }
    registerTehutiSensor(registerTehutiSensorFn, tehutiMetricNameEnum, tehutiMetricStats);
  }

  /** Package-private so async gauges are only created through {@link AsyncMetricEntityStateBase#createWithState}. */
  <S> AsyncMetricEntityState(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      LiveStateResolver<S> liveStateResolver,
      ValueResolver<S> valueResolver,
      Attributes asyncAttributes) {
    this(
        metricEntity,
        otelRepository,
        baseDimensionsMap,
        registerTehutiSensorFn,
        tehutiMetricNameEnum,
        tehutiMetricStats,
        false);
    validateAsyncCallback(liveStateResolver != null && valueResolver != null);
    if (metricEntity.getMetricType() != MetricType.ASYNC_GAUGE
        && metricEntity.getMetricType() != MetricType.ASYNC_DOUBLE_GAUGE) {
      throw new IllegalArgumentException(
          "State-based async gauges require ASYNC_GAUGE or ASYNC_DOUBLE_GAUGE for metric: "
              + metricEntity.getMetricName());
    }
    if (emitOpenTelemetryMetrics()) {
      setOtelMetric(
          otelRepository.registerObservableGauge(
              this.metricEntity,
              observation -> observation.observe(asyncAttributes, liveStateResolver, valueResolver)));
    }
    registerTehutiSensor(registerTehutiSensorFn, tehutiMetricNameEnum, tehutiMetricStats);
  }

  /** Common field initialization for all constructors. */
  private AsyncMetricEntityState(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      boolean ignored) {
    this.metricEntity = metricEntity;
    this.emitOpenTelemetryMetrics = otelRepository != null && otelRepository.emitOpenTelemetryMetrics();
    this.emitTehutiMetrics =
        shouldEmitTehutiMetrics(otelRepository, registerTehutiSensorFn, tehutiMetricNameEnum, tehutiMetricStats);
    this.otelRepository = otelRepository;
    this.baseDimensionsMap = baseDimensionsMap;
    validateTehutiAsyncGaugeConsistency(tehutiMetricStats);
  }

  /**
   * emit Tehuti metrics only when
   * 1. registerTehutiSensorFn, tehutiMetricNameEnum and tehutiMetricStats are provided
   * 2. otelRepository is null (otel is not enabled) or otelRepository.emitTehutiMetrics() is true
   */
  private boolean shouldEmitTehutiMetrics(
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats) {
    return registerTehutiSensorFn != null && tehutiMetricNameEnum != null && tehutiMetricStats != null
        && !tehutiMetricStats.isEmpty() && (otelRepository == null || otelRepository.emitTehutiMetrics());
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
   * Validates that async callback presence is consistent with the metric type.
   *
   * @param hasAsyncCallback whether an async gauge callback was provided
   */
  private void validateAsyncCallback(boolean hasAsyncCallback) {
    if (hasAsyncCallback && !metricEntity.getMetricType().isAsyncMetric()) {
      throw new IllegalArgumentException(
          "Async callback is provided, but the metric type is not async for metric: " + metricEntity.getMetricName());
    } else if (metricEntity.getMetricType().isAsyncMetric() && !hasAsyncCallback
        && !metricEntity.getMetricType().isObservableCounterType()) {
      // Observable counter types (ASYNC_COUNTER_FOR_HIGH_PERF_CASES and ASYNC_UP_DOWN_COUNTER_FOR_HIGH_PERF_CASES)
      // are async but handle callback registration internally via registerObservableLongCounter/UpDownCounter(),
      // so they don't need a callback passed in here.
      throw new IllegalArgumentException(
          "Async callback is not provided, but the metric type is async for metric: " + metricEntity.getMetricName());
    }
  }

  /**
   * Validates that Tehuti AsyncGauge stats are consistent with the OTel metric type.
   * Called once during construction after field initialization.
   */
  private void validateTehutiAsyncGaugeConsistency(List<MeasurableStat> tehutiMetricStats) {
    if (tehutiMetricStats == null || tehutiMetricStats.isEmpty()) {
      return;
    }
    if (tehutiMetricStats.stream().anyMatch(stat -> stat instanceof AsyncGauge)) {
      if (tehutiMetricStats.size() > 1) {
        throw new IllegalArgumentException(
            "Tehuti metric stats contains AsyncGauge, but it should be the only stat for metric: "
                + metricEntity.getMetricName());
      }
      MetricType mt = metricEntity.getMetricType();
      if (mt != MetricType.ASYNC_GAUGE && mt != MetricType.ASYNC_DOUBLE_GAUGE) {
        throw new IllegalArgumentException(
            "Tehuti metric stats contains AsyncGauge, but the otel metric type is not ASYNC_GAUGE/ASYNC_DOUBLE_GAUGE for metric: "
                + metricEntity.getMetricName());
      }
    } else if (metricEntity.getMetricType() == MetricType.ASYNC_GAUGE
        || metricEntity.getMetricType() == MetricType.ASYNC_DOUBLE_GAUGE) {
      throw new IllegalArgumentException(
          "Tehuti metric stats does not contain AsyncGauge, but the otel metric type is ASYNC_GAUGE/ASYNC_DOUBLE_GAUGE for metric: "
              + metricEntity.getMetricName());
    }
  }

  /** Registers the Tehuti sensor if Tehuti metrics are enabled. */
  private void registerTehutiSensor(
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats) {
    if (emitTehutiMetrics()) {
      setTehutiSensor(
          registerTehutiSensorFn
              .register(tehutiMetricNameEnum.getMetricName(), tehutiMetricStats.toArray(new MeasurableStat[0])));
    }
  }

  /**
   * Validate that the {@link AsyncMetricEntityState} has all required dimensions passed in as defined in
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
                "baseAttributes: " + baseAttributes.asMap() + " should contain key: " + key.getKey()
                    + ", and should contain all the keys and same values as in baseDimensionsMap: "
                    + baseDimensionsMap);
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

  final boolean emitTehutiMetrics() {
    return emitTehutiMetrics;
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

  public Sensor getTehutiSensor() {
    return tehutiSensor;
  }

  public Object getOtelMetric() {
    return otelMetric;
  }

  /**
   * Stops OTel reporting. Gauges stop at once. Observable counters retire from their own callback after
   * the next collection reports their final totals, so increments since the previous export are kept.
   */
  @Override
  public void close() {
    if (closed.compareAndSet(false, true) && !metricEntity.getMetricType().isObservableCounterType()) {
      closeOtelInstrument();
    }
  }

  final boolean isClosed() {
    return closed.get();
  }

  /** Unregisters the observable instrument, if any, so the SDK stops invoking its callback. */
  final void closeOtelInstrument() {
    if (!emitOpenTelemetryMetrics() || otelRepository == null || otelMetric == null) {
      return;
    }
    MetricType metricType = metricEntity.getMetricType();
    if (metricType.isObservableCounterType() || metricType == MetricType.ASYNC_GAUGE
        || metricType == MetricType.ASYNC_DOUBLE_GAUGE) {
      otelRepository.closeObservableInstrument(metricEntity, otelMetric);
      otelMetric = null;
    }
  }
}
