package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.stats.metrics.MetricUnit;


/**
 * Dimension enum for adaptive throttler types. {@code AdaptiveThrottlingServiceStats} eagerly
 * creates a joint Tehuti+OTel metric state for every enum value at construction time. The enum
 * constant name (lowercased) is used as the OTel dimension value.
 *
 * <p>Each throttler type carries a {@link MetricUnit} indicating what it measures:
 * {@link MetricUnit#NUMBER} for record-count throttlers and {@link MetricUnit#BYTES} for
 * bandwidth throttlers. This is used by {@code AdaptiveThrottlingServiceStats} to route
 * recordings to the appropriate OTel metric entity ({@code RECORD_COUNT} vs {@code BYTE_COUNT}).
 *
 * <p>Only {@link MetricUnit#NUMBER} and {@link MetricUnit#BYTES} are currently supported.
 * Adding a new unit requires updating {@code AdaptiveThrottlingServiceStats.getMetricEntityForType()}.
 */
public enum VeniceAdaptiveThrottlerType implements VeniceDimensionInterface {
  PUBSUB_CONSUMPTION_RECORDS_COUNT(MetricUnit.NUMBER), PUBSUB_CONSUMPTION_BANDWIDTH(MetricUnit.BYTES),
  CURRENT_VERSION_AA_WC_LEADER_RECORDS_COUNT(MetricUnit.NUMBER),
  CURRENT_VERSION_NON_AA_WC_LEADER_RECORDS_COUNT(MetricUnit.NUMBER),
  NON_CURRENT_VERSION_AA_WC_LEADER_RECORDS_COUNT(MetricUnit.NUMBER),
  NON_CURRENT_VERSION_NON_AA_WC_LEADER_RECORDS_COUNT(MetricUnit.NUMBER);

  private final MetricUnit metricUnit;

  VeniceAdaptiveThrottlerType(MetricUnit metricUnit) {
    this.metricUnit = metricUnit;
  }

  /** Returns {@link MetricUnit#NUMBER} for record-count throttlers, {@link MetricUnit#BYTES} for bandwidth. */
  public MetricUnit getMetricUnit() {
    return metricUnit;
  }

  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_ADAPTIVE_THROTTLER_TYPE;
  }
}
