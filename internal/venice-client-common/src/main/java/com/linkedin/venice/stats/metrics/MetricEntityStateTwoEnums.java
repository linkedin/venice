package com.linkedin.venice.stats.metrics;

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
 * Similar to {@link MetricEntityStateOneEnum} but with two dynamic dimensions and 2 level EnumMap
 */
public class MetricEntityStateTwoEnums<E1 extends Enum<E1> & VeniceDimensionInterface, E2 extends Enum<E2> & VeniceDimensionInterface>
    extends MetricEntityState {
  private final EnumMap<E1, EnumMap<E2, MetricAttributesData>> metricAttributesDataEnumMap;
  private final Class<E1> enumTypeClass1;
  private final Class<E2> enumTypeClass2;

  /** should not be called directly, call {@link #create} instead */
  private MetricEntityStateTwoEnums(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E1> enumTypeClass1,
      Class<E2> enumTypeClass2) {
    this(
        metricEntity,
        otelRepository,
        null,
        null,
        Collections.EMPTY_LIST,
        baseDimensionsMap,
        enumTypeClass1,
        enumTypeClass2);
  }

  /** should not be called directly, call {@link #create} instead */
  private MetricEntityStateTwoEnums(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E1> enumTypeClass1,
      Class<E2> enumTypeClass2) {
    super(
        metricEntity,
        otelRepository,
        baseDimensionsMap,
        registerTehutiSensorFn,
        tehutiMetricNameEnum,
        tehutiMetricStats);
    validateRequiredDimensions(metricEntity, null, baseDimensionsMap, enumTypeClass1, enumTypeClass2);
    this.enumTypeClass1 = enumTypeClass1;
    this.enumTypeClass2 = enumTypeClass2;
    this.metricAttributesDataEnumMap = createMetricAttributesDataEnumMap();
    registerObservableCounterIfNeeded();
  }

  /** Factory method with named parameters to ensure the passed in enumTypeClass are in the same order as E */
  public static <E1 extends Enum<E1> & VeniceDimensionInterface, E2 extends Enum<E2> & VeniceDimensionInterface> MetricEntityStateTwoEnums<E1, E2> create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E1> enumTypeClass1,
      Class<E2> enumTypeClass2) {
    return new MetricEntityStateTwoEnums<>(
        metricEntity,
        otelRepository,
        baseDimensionsMap,
        enumTypeClass1,
        enumTypeClass2);
  }

  /** Overloaded Factory method for constructor with Tehuti parameters */
  public static <E1 extends Enum<E1> & VeniceDimensionInterface, E2 extends Enum<E2> & VeniceDimensionInterface> MetricEntityStateTwoEnums<E1, E2> create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E1> enumTypeClass1,
      Class<E2> enumTypeClass2) {
    return new MetricEntityStateTwoEnums<>(
        metricEntity,
        otelRepository,
        registerTehutiSensorFn,
        tehutiMetricNameEnum,
        tehutiMetricStats,
        baseDimensionsMap,
        enumTypeClass1,
        enumTypeClass2);
  }

  /**
   * Creates an EnumMap of {@link MetricAttributesData} which will be used to lazy initialize the
   * Attributes and optionally a LongAdder for ASYNC_COUNTER_FOR_HIGH_PERF_CASES metrics.
   */
  private EnumMap<E1, EnumMap<E2, MetricAttributesData>> createMetricAttributesDataEnumMap() {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }

    return new EnumMap<>(enumTypeClass1);
  }

  /**
   * Manages the nested EnumMap structure for lazy initialization of MetricAttributesData.
   * The structure is a two-level nested EnumMap: EnumMap<E1, EnumMap<E2, MetricAttributesData>>.
   * This allows efficient retrieval of state based on two enum dimensions (E1, E2).
   *
   * For thread safety considerations, refer {@link MetricEntityStateOneEnum#getMetricAttributesData}.
   */
  private MetricAttributesData getMetricAttributesData(E1 dimension1, E2 dimension2) {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }

    return metricAttributesDataEnumMap.computeIfAbsent(dimension1, k -> {
      validateInputDimension(k);
      return new EnumMap<>(enumTypeClass2);
    }).computeIfAbsent(dimension2, k -> {
      validateInputDimension(k);
      Attributes attrs = createAttributes(dimension1, dimension2);
      return new MetricAttributesData(attrs, isObservableCounter());
    });
  }

  /**
   * Returns the Attributes for the given dimensions.
   */
  public Attributes getAttributes(E1 dimension1, E2 dimension2) {
    MetricAttributesData holder = getMetricAttributesData(dimension1, dimension2);
    return holder != null ? holder.getAttributes() : null;
  }

  public void record(double value, @Nonnull E1 dimension1, @Nonnull E2 dimension2) {
    super.record(value, getMetricAttributesData(dimension1, dimension2));
  }

  public void record(long value, @Nonnull E1 dimension1, @Nonnull E2 dimension2) {
    super.record(value, getMetricAttributesData(dimension1, dimension2));
  }

  @Override
  protected Iterable<MetricAttributesData> getAllMetricAttributesData() {
    if (metricAttributesDataEnumMap == null) {
      return null;
    }

    List<MetricAttributesData> allData = new ArrayList<>();
    for (EnumMap<E2, MetricAttributesData> level2Map: metricAttributesDataEnumMap.values()) {
      allData.addAll(level2Map.values());
    }
    return allData;
  }

  /** visible for testing */
  public EnumMap<E1, EnumMap<E2, MetricAttributesData>> getMetricAttributesDataEnumMap() {
    return metricAttributesDataEnumMap;
  }
}
