package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MeasurableStat;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * Similar to {@link MetricEntityStateOneEnum} but with Four dynamic dimensions and 4 level EnumMap
 */
public class MetricEntityStateFourEnums<E1 extends Enum<E1> & VeniceDimensionInterface, E2 extends Enum<E2> & VeniceDimensionInterface, E3 extends Enum<E3> & VeniceDimensionInterface, E4 extends Enum<E4> & VeniceDimensionInterface>
    extends MetricEntityState {
  private final EnumMap<E1, EnumMap<E2, EnumMap<E3, EnumMap<E4, Attributes>>>> attributesEnumMap;

  private final Class<E1> enumTypeClass1;
  private final Class<E2> enumTypeClass2;
  private final Class<E3> enumTypeClass3;
  private final Class<E4> enumTypeClass4;

  /** should not be called directly, call {@link #create} instead */
  private MetricEntityStateFourEnums(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E1> enumTypeClass1,
      Class<E2> enumTypeClass2,
      Class<E3> enumTypeClass3,
      Class<E4> enumTypeClass4) {
    this(
        metricEntity,
        otelRepository,
        null,
        null,
        Collections.EMPTY_LIST,
        baseDimensionsMap,
        enumTypeClass1,
        enumTypeClass2,
        enumTypeClass3,
        enumTypeClass4);
  }

  /** should not be called directly, call {@link #create} instead */
  private MetricEntityStateFourEnums(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E1> enumTypeClass1,
      Class<E2> enumTypeClass2,
      Class<E3> enumTypeClass3,
      Class<E4> enumTypeClass4) {
    super(
        metricEntity,
        otelRepository,
        baseDimensionsMap,
        registerTehutiSensorFn,
        tehutiMetricNameEnum,
        tehutiMetricStats);
    validateRequiredDimensions(
        metricEntity,
        null,
        baseDimensionsMap,
        enumTypeClass1,
        enumTypeClass2,
        enumTypeClass3,
        enumTypeClass4);
    this.enumTypeClass1 = enumTypeClass1;
    this.enumTypeClass2 = enumTypeClass2;
    this.enumTypeClass3 = enumTypeClass3;
    this.enumTypeClass4 = enumTypeClass4;
    this.attributesEnumMap = createAttributesEnumMap();
  }

  /** Factory method with named parameters to ensure the passed in enumTypeClass are in the same order as E */
  public static <E1 extends Enum<E1> & VeniceDimensionInterface, E2 extends Enum<E2> & VeniceDimensionInterface, E3 extends Enum<E3> & VeniceDimensionInterface, E4 extends Enum<E4> & VeniceDimensionInterface> MetricEntityStateFourEnums<E1, E2, E3, E4> create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E1> enumTypeClass1,
      Class<E2> enumTypeClass2,
      Class<E3> enumTypeClass3,
      Class<E4> enumTypeClass4) {
    return new MetricEntityStateFourEnums<>(
        metricEntity,
        otelRepository,
        baseDimensionsMap,
        enumTypeClass1,
        enumTypeClass2,
        enumTypeClass3,
        enumTypeClass4);
  }

  /** Overloaded Factory method for constructor with Tehuti parameters */
  public static <E1 extends Enum<E1> & VeniceDimensionInterface, E2 extends Enum<E2> & VeniceDimensionInterface, E3 extends Enum<E3> & VeniceDimensionInterface, E4 extends Enum<E4> & VeniceDimensionInterface> MetricEntityStateFourEnums<E1, E2, E3, E4> create(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Class<E1> enumTypeClass1,
      Class<E2> enumTypeClass2,
      Class<E3> enumTypeClass3,
      Class<E4> enumTypeClass4) {
    return new MetricEntityStateFourEnums<>(
        metricEntity,
        otelRepository,
        registerTehutiSensorFn,
        tehutiMetricNameEnum,
        tehutiMetricStats,
        baseDimensionsMap,
        enumTypeClass1,
        enumTypeClass2,
        enumTypeClass3,
        enumTypeClass4);
  }

  /**
   * Creates an EnumMap of {@link Attributes} which will be used to lazy initialize the Attributes
   */
  private EnumMap<E1, EnumMap<E2, EnumMap<E3, EnumMap<E4, Attributes>>>> createAttributesEnumMap() {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }

    return new EnumMap<>(enumTypeClass1);
  }

  /**
   * Manages the nested EnumMap structure for lazy initialization of Attributes.
   * The structure is a Four-level nested EnumMap:
   * EnumMap<E1, EnumMap<E2, EnumMap<E3, EnumMap<E4, Attributes>>>>.
   * This allows efficient retrieval of Attributes based on Four enum dimensions (E1, E2, E3, E4).
   *
   * For thread safety considerations, refer {@link MetricEntityStateOneEnum#getAttributes}.
   */
  public Attributes getAttributes(E1 dimension1, E2 dimension2, E3 dimension3, E4 dimension4) {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }

    Attributes attributes = attributesEnumMap.computeIfAbsent(dimension1, k -> {
      validateInputDimension(k);
      return new EnumMap<>(enumTypeClass2);
    }).computeIfAbsent(dimension2, k -> {
      validateInputDimension(k);
      return new EnumMap<>(enumTypeClass3);
    }).computeIfAbsent(dimension3, k -> {
      validateInputDimension(k);
      return new EnumMap<>(enumTypeClass4);
    }).computeIfAbsent(dimension4, k -> {
      validateInputDimension(k);
      return createAttributes(dimension1, dimension2, dimension3, dimension4);
    });

    if (attributes == null) {
      throw new IllegalArgumentException(
          "No Attributes found for dimensions: " + dimension1 + "," + dimension2 + "," + dimension3 + "," + dimension4
              + " for metric Entity: " + getMetricEntity().getMetricName());
    }
    return attributes;
  }

  public void record(
      long value,
      @Nonnull E1 dimension1,
      @Nonnull E2 dimension2,
      @Nonnull E3 dimension3,
      @Nonnull E4 dimension4) {
    super.record(value, getAttributes(dimension1, dimension2, dimension3, dimension4));
  }

  public void record(
      double value,
      @Nonnull E1 dimension1,
      @Nonnull E2 dimension2,
      @Nonnull E3 dimension3,
      @Nonnull E4 dimension4) {
    super.record(value, getAttributes(dimension1, dimension2, dimension3, dimension4));
  }

  /** visible for testing */
  public EnumMap<E1, EnumMap<E2, EnumMap<E3, EnumMap<E4, Attributes>>>> getAttributesEnumMap() {
    return attributesEnumMap;
  }
}
