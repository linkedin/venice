package com.linkedin.venice.stats.dimensions;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


/**
 * Test fixture for validating enums implementing {@link VeniceDimensionInterface}. Create an
 * instance with the enum class, expected dimension name, and expected value mappings, then call
 * {@link #assertAll()} to run all validations, or call {@link #assertDimensionCount()},
 * {@link #assertDimensionValues()}, and {@link #assertNoDuplicateDimensionValues()} individually
 * for targeted checks.
 *
 * @param <T> the enum class under test
 */
public final class VeniceDimensionTestFixture<T extends Enum<T> & VeniceDimensionInterface> {
  private final Class<T> enumClass;
  private final VeniceMetricsDimensions expectedDimensionName;
  private final Map<T, String> expectedDimensionValueMapping;

  public VeniceDimensionTestFixture(
      Class<T> enumClass,
      VeniceMetricsDimensions expectedDimensionName,
      Map<T, String> expectedDimensionValueMapping) {
    this.enumClass = Objects.requireNonNull(enumClass);
    this.expectedDimensionName = Objects.requireNonNull(expectedDimensionName);
    this.expectedDimensionValueMapping = Objects.requireNonNull(expectedDimensionValueMapping);
  }

  /** Runs all assertions: validates enum value count, dimension values, and uniqueness of dimension values. */
  public void assertAll() {
    assertDimensionCount();
    assertDimensionValues();
    assertNoDuplicateDimensionValues();
  }

  public void assertDimensionCount() {
    assertEquals(
        enumClass.getEnumConstants().length,
        expectedDimensionValueMapping.size(),
        "Enum value count does not match expected mappings. "
            + "Add or remove entries in expectedDimensionValueMapping when modifying " + enumClass.getSimpleName());
  }

  public void assertDimensionValues() {
    for (T enumValue: enumClass.getEnumConstants()) {
      String expectedValue = expectedDimensionValueMapping.get(enumValue);
      assertNotNull(expectedValue, "No expected value found for " + enumValue);
      assertEquals(enumValue.getDimensionValue(), expectedValue, "Dimension value mismatch for " + enumValue);
      assertEquals(
          enumValue.getDimensionName(),
          expectedDimensionName,
          "Dimension name mismatch for " + enumValue + "; all enum values should share the same dimension name");
    }
  }

  public void assertNoDuplicateDimensionValues() {
    Set<String> seen = new HashSet<>();
    for (T enumValue: enumClass.getEnumConstants()) {
      assertTrue(
          seen.add(enumValue.getDimensionValue()),
          "Duplicate dimension value '" + enumValue.getDimensionValue() + "' found for " + enumValue + " in "
              + enumClass.getSimpleName());
    }
  }
}
