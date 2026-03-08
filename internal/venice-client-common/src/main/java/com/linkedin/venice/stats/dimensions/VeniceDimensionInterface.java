package com.linkedin.venice.stats.dimensions;

/**
 * Every enum that should be used as a dimension for otel should implement this interface
 * as this mandates the enum to have a dimension name and a dimension value.
 *
 * All such enums should add a test class that uses {@code VeniceDimensionTestFixture} to
 * validate that the enum value count matches expected mappings, that all instances share
 * the same dimension name, and that each instance's dimension value matches the expected
 * value.
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
   * Default implementation returns {@code name().toLowerCase()}, which is the convention
   * for most dimension enums. Override only when the dimension value differs from the
   * lowercase enum constant name (e.g., HTTP status codes, custom string mappings).
   *
   * <p>This default relies on all implementors being enums. If this interface is ever
   * implemented by a non-enum class, this method MUST be overridden — the default will
   * throw {@link ClassCastException}.
   */
  default String getDimensionValue() {
    return ((Enum<?>) this).name().toLowerCase();
  }
}
