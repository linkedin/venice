package com.linkedin.venice.stats.dimensions;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import java.util.Map;
import org.testng.annotations.Test;


/**
 * Abstract class to generically test all the assumptions for enums which implement the
 * {@link VeniceDimensionInterface} interface. Subclasses need to implement the constructor and the
 * abstract function and maybe add some specific tests needed for this class.
 *
 * @param <T> the enum class under test
 */
@Test
public abstract class VeniceDimensionInterfaceTest<T extends VeniceDimensionInterface> {
  private final Class<T> enumClass;

  protected VeniceDimensionInterfaceTest(Class<T> enumClass) {
    this.enumClass = enumClass;
  }

  /**
   * Returns the expected dimension name for the enum class under test.
   * This should be a single value for all the enum instances.
   */
  protected abstract VeniceMetricsDimensions expectedDimensionName();

  /**
   * Returns the expected dimension value mapping for the enum class under test.
   * This should be a mapping of each enum instance to its expected dimension value.
   */
  protected abstract Map<T, String> expectedDimensionValueMapping();

  @Test
  public void test() {
    VeniceMetricsDimensions expectedDimensionName = expectedDimensionName();
    assertNotNull(expectedDimensionName);

    Map<T, String> expectedDimensionValueMapping = expectedDimensionValueMapping();
    assertFalse(expectedDimensionValueMapping.isEmpty());

    // loop through all the enum values in enumClass and check it against the expected mapping
    for (T enumValue: enumClass.getEnumConstants()) {
      String expectedDimensionValue = expectedDimensionValueMapping.get(enumValue);
      assertNotNull(expectedDimensionValue, "No expected value found for " + enumValue);
      assertEquals(enumValue.getDimensionValue(), expectedDimensionValue);

      // all enums should have the same dimension name
      assertEquals(enumValue.getDimensionName(), expectedDimensionName);
    }
  }
}
