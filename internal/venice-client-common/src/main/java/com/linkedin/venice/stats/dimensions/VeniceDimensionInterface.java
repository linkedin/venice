package com.linkedin.venice.stats.dimensions;

/**
 * Every enum that should be used as a dimension for otel should implement this interface
 * as this mandates the enum to have a dimension name and a dimension value.
 */
public interface VeniceDimensionInterface {
  VeniceMetricsDimensions getDimensionName();

  String getDimensionValue();
}
