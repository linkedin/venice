package com.linkedin.venice.stats.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MeasurableStat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * Similar to {@link MetricEntityStateOneEnum} but with three dynamic dimensions and 3 level EnumMap
 */
public class MetricEntityStateThreeEnums<E1 extends Enum<E1> & VeniceDimensionInterface, E2 extends Enum<E2> & VeniceDimensionInterface, E3 extends Enum<E3> & VeniceDimensionInterface>
    extends MetricEntityState {
  private final EnumMap<E1, EnumMap<E2, EnumMap<E3, MetricAttributesData>>> metricAttributesDataEnumMap;

  private final Class<E1> enumTypeClass1;
  private final Class<E2> enumTypeClass2;
  private final Class<E3> enumTypeClass3;

  /** should not be called directly, call {@link #create} instead */
  private MetricEntityStateThreeEnums(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E1> enumTypeClass1,
      Class<E2> enumTypeClass2,
      Class<E3> enumTypeClass3) {
    this(
        metricEntity,
        otelRepository,
        null,
        null,
        Collections.EMPTY_LIST,
        baseDimensionsMap,
        enumTypeClass1,
        enumTypeClass2,
        enumTypeClass3);
  }

  /** should not be called directly, call {@link #create} instead */
  private MetricEntityStateThreeEnums(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E1> enumTypeClass1,
      Class<E2> enumTypeClass2,
      Class<E3> enumTypeClass3) {
    super(
        metricEntity,
        otelRepository,
        baseDimensionsMap,
        registerTehutiSensorFn,
        tehutiMetricNameEnum,
        tehutiMetricStats);
    validateRequiredDimensions(metricEntity, null, baseDimensionsMap, enumTypeClass1, enumTypeClass2, enumTypeClass3);
    this.enumTypeClass1 = enumTypeClass1;
    this.enumTypeClass2 = enumTypeClass2;
    this.enumTypeClass3 = enumTypeClass3;
    this.metricAttributesDataEnumMap = createMetricAttributesDataEnumMap();
    registerObservableCounterIfNeeded();
  }

  /** Factory method with named parameters to ensure the passed in enumTypeClass are in the same order as E */
  public static <E1 extends Enum<E1> & VeniceDimensionInterface, E2 extends Enum<E2> & VeniceDimensionInterface, E3 extends Enum<E3> & VeniceDimensionInterface> MetricEntityStateThreeEnums<E1, E2, E3> create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E1> enumTypeClass1,
      Class<E2> enumTypeClass2,
      Class<E3> enumTypeClass3) {
    return new MetricEntityStateThreeEnums<>(
        metricEntity,
        otelRepository,
        baseDimensionsMap,
        enumTypeClass1,
        enumTypeClass2,
        enumTypeClass3);
  }

  /** Overloaded Factory method for constructor with Tehuti parameters */
  public static <E1 extends Enum<E1> & VeniceDimensionInterface, E2 extends Enum<E2> & VeniceDimensionInterface, E3 extends Enum<E3> & VeniceDimensionInterface> MetricEntityStateThreeEnums<E1, E2, E3> create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E1> enumTypeClass1,
      Class<E2> enumTypeClass2,
      Class<E3> enumTypeClass3) {
    return new MetricEntityStateThreeEnums<>(
        metricEntity,
        otelRepository,
        registerTehutiSensorFn,
        tehutiMetricNameEnum,
        tehutiMetricStats,
        baseDimensionsMap,
        enumTypeClass1,
        enumTypeClass2,
        enumTypeClass3);
  }

  /**
   * Creates an EnumMap of {@link MetricAttributesData} which will be used to lazy initialize the
   * Attributes and optionally a LongAdder for ASYNC_COUNTER_FOR_HIGH_PERF_CASES metrics.
   */
  private EnumMap<E1, EnumMap<E2, EnumMap<E3, MetricAttributesData>>> createMetricAttributesDataEnumMap() {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }

    return new EnumMap<>(enumTypeClass1);
  }

  /**
   * Manages the nested EnumMap structure for lazy initialization of MetricAttributesData.
   * The structure is a three-level nested EnumMap: EnumMap<E1, EnumMap<E2, EnumMap<E3, MetricAttributesData>>>.
   * This allows efficient retrieval of state based on three enum dimensions (E1, E2, E3).
   *
   * For thread safety considerations, refer {@link MetricEntityStateOneEnum#getAttributes}.
   */
  private MetricAttributesData getMetricAttributesData(E1 dimension1, E2 dimension2, E3 dimension3) {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }

    return metricAttributesDataEnumMap.computeIfAbsent(dimension1, k -> {
      validateInputDimension(k);
      return new EnumMap<>(enumTypeClass2);
    }).computeIfAbsent(dimension2, k -> {
      validateInputDimension(k);
      return new EnumMap<>(enumTypeClass3);
    }).computeIfAbsent(dimension3, k -> {
      validateInputDimension(k);
      Attributes attrs = createAttributes(dimension1, dimension2, dimension3);
      return new MetricAttributesData(attrs, isObservableCounter());
    });
  }

  @VisibleForTesting
  /**
   * Returns the Attributes for the given dimensions.
   */
  public Attributes getAttributes(E1 dimension1, E2 dimension2, E3 dimension3) {
    MetricAttributesData holder = getMetricAttributesData(dimension1, dimension2, dimension3);
    return holder != null ? holder.getAttributes() : null;
  }

  /**
   * Records a value for the given dimensions.
   * <p>
   * For {@link MetricType#ASYNC_COUNTER_FOR_HIGH_PERF_CASES} metrics, this uses the internal LongAdder for fast,
   * contention-free recording. The accumulated value is read during OTel's collection callback.
   * <p>
   * For other metric types, this delegates to the parent class's record method.
   */
  public void record(double value, @Nonnull E1 dimension1, @Nonnull E2 dimension2, @Nonnull E3 dimension3) {
    super.record(value, getMetricAttributesData(dimension1, dimension2, dimension3));
  }

  public void record(long value, @Nonnull E1 dimension1, @Nonnull E2 dimension2, @Nonnull E3 dimension3) {
    super.record(value, getMetricAttributesData(dimension1, dimension2, dimension3));
  }

  @Override
  protected Iterable<MetricAttributesData> getAllMetricAttributesData() {
    if (metricAttributesDataEnumMap == null) {
      return null;
    }

    List<MetricAttributesData> allData = new ArrayList<>();
    for (EnumMap<E2, EnumMap<E3, MetricAttributesData>> level2Map: metricAttributesDataEnumMap.values()) {
      for (EnumMap<E3, MetricAttributesData> level3Map: level2Map.values()) {
        allData.addAll(level3Map.values());
      }
    }
    return allData;
  }

  /** visible for testing */
  public EnumMap<E1, EnumMap<E2, EnumMap<E3, MetricAttributesData>>> getMetricAttributesDataEnumMap() {
    return metricAttributesDataEnumMap;
  }
}
