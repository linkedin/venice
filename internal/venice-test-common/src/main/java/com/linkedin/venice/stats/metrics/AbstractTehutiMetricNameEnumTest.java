package com.linkedin.venice.stats.metrics;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Map;
import org.testng.annotations.Test;


/**
 * Abstract test for enums implementing {@link TehutiMetricNameEnum}. Subclasses provide the
 * enum class and a map of expected metric name strings.
 *
 * <p>Modeled after
 * {@link com.linkedin.venice.stats.dimensions.VeniceDimensionInterfaceTest}.
 *
 * @param <T> the enum class under test
 */
@Test
public abstract class AbstractTehutiMetricNameEnumTest<T extends Enum<T> & TehutiMetricNameEnum> {
  private final Class<T> enumClass;

  protected AbstractTehutiMetricNameEnumTest(Class<T> enumClass) {
    this.enumClass = enumClass;
  }

  /**
   * Returns a map from each enum constant to its expected metric name string.
   * Every enum value must have an entry; missing entries fail the test.
   */
  protected abstract Map<T, String> expectedMetricNames();

  @Test
  public void testMetricNameCount() {
    assertEquals(
        enumClass.getEnumConstants().length,
        expectedMetricNames().size(),
        "Enum value count does not match expected metric names. "
            + "Add or remove entries in expectedMetricNames() when modifying " + enumClass.getSimpleName());
  }

  @Test
  public void testMetricNames() {
    Map<T, String> expected = expectedMetricNames();
    for (T enumValue: enumClass.getEnumConstants()) {
      String expectedName = expected.get(enumValue);
      assertNotNull(expectedName, "No expected metric name for " + enumValue.toString());
      assertEquals(enumValue.getMetricName(), expectedName, "Metric name mismatch for " + enumValue.toString());
    }
  }
}
