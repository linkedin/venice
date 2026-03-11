package com.linkedin.venice.stats.metrics;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


/**
 * Test fixture for validating enums implementing {@link TehutiMetricNameEnum}. Create an instance
 * with the enum class and expected metric name strings, then call {@link #assertAll()} or
 * individual assertion methods.
 *
 * @param <T> the enum class under test
 */
public final class TehutiMetricNameEnumTestFixture<T extends Enum<T> & TehutiMetricNameEnum> {
  private final Class<T> enumClass;
  private final Map<T, String> expectedMetricNames;

  public TehutiMetricNameEnumTestFixture(Class<T> enumClass, Map<T, String> expectedMetricNames) {
    this.enumClass = Objects.requireNonNull(enumClass);
    this.expectedMetricNames = Objects.requireNonNull(expectedMetricNames);
  }

  /** Runs all assertions: count, duplicate names, and metric names. */
  public void assertAll() {
    assertMetricNameCount();
    assertNoDuplicateMetricNames();
    assertMetricNames();
  }

  public void assertMetricNameCount() {
    assertEquals(
        enumClass.getEnumConstants().length,
        expectedMetricNames.size(),
        "Enum value count does not match expected metric names. "
            + "Add or remove entries in expectedMetricNames when modifying " + enumClass.getSimpleName());
  }

  public void assertNoDuplicateMetricNames() {
    Set<String> seen = new HashSet<>();
    for (T enumValue: enumClass.getEnumConstants()) {
      String name = enumValue.getMetricName();
      assertTrue(
          seen.add(name),
          "Duplicate metric name '" + name + "' in " + enumClass.getSimpleName()
              + ". Each enum constant must have a unique metric name.");
    }
  }

  public void assertMetricNames() {
    for (T enumValue: enumClass.getEnumConstants()) {
      String expectedName = expectedMetricNames.get(enumValue);
      assertNotNull(expectedName, "No expected metric name for " + enumValue.toString());
      assertEquals(enumValue.getMetricName(), expectedName, "Metric name mismatch for " + enumValue.toString());
    }
  }
}
