package com.linkedin.venice.stats.dimensions;

/**
 * Every enum that should be used as a dimension for otel should implement this interface
 * as this mandates the enum to have a dimension name and a dimension value.
 *
 * All such enums should add a test class that extends {@link VeniceDimensionInterfaceTest} to
 * test whether all the enum instances of an enum Class have the same dimension name and also
 * validates the dimension values.
 */
public interface VeniceDimensionInterface {
  /**
   * Dimension name: Returns the {@link VeniceMetricsDimensions} for the enum. All the
   * instances of a Enum class should have the same dimension name. Ideally this could
   * have been a static variable/method in the Enum class, but to enforce having this
   * method via this interface, it is made as a non-static method.
   */
  VeniceMetricsDimensions getDimensionName();

  /**
   * Dimension value: Returns the dimension value for each enum instance.
   */
  String getDimensionValue();
}
