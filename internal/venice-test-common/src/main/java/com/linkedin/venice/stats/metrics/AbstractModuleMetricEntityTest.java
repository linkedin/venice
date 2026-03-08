package com.linkedin.venice.stats.metrics;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;


/**
 * Abstract test for enums implementing {@link ModuleMetricEntityInterface}. Subclasses provide the
 * enum class and expected definitions. Registration in aggregated collections (e.g.,
 * {@code SERVER_METRIC_ENTITIES}) is validated by classpath-scanning tests like
 * {@code ServerMetricEntityTest}, not here.
 *
 * <p>Modeled after
 * {@link com.linkedin.venice.stats.dimensions.VeniceDimensionInterfaceTest}.
 *
 * @param <T> the enum class under test
 */
public abstract class AbstractModuleMetricEntityTest<T extends Enum<T> & ModuleMetricEntityInterface> {
  private final Class<T> enumClass;

  protected AbstractModuleMetricEntityTest(Class<T> enumClass) {
    this.enumClass = enumClass;
  }

  /**
   * Returns a map from each enum constant to its expected {@link MetricEntityExpectation}.
   * Every enum value must have an entry; missing entries fail the test.
   */
  protected abstract Map<T, MetricEntityExpectation> expectedDefinitions();

  @Test
  public void testMetricEntityCount() {
    assertEquals(
        enumClass.getEnumConstants().length,
        expectedDefinitions().size(),
        "Enum value count does not match expected definitions. "
            + "Add or remove entries in expectedDefinitions() when modifying " + enumClass.getSimpleName());
  }

  @Test
  public void testMetricEntityDefinitions() {
    Map<T, MetricEntityExpectation> expected = expectedDefinitions();
    for (T enumValue: enumClass.getEnumConstants()) {
      MetricEntityExpectation exp = expected.get(enumValue);
      assertNotNull(exp, "No expected definition for " + enumValue.toString());
      MetricEntity entity = enumValue.getMetricEntity();
      assertNotNull(entity, "getMetricEntity() returned null for " + enumValue.toString());
      assertEquals(entity.getMetricName(), exp.name, "Metric name mismatch for " + enumValue.toString());
      assertEquals(entity.getMetricType(), exp.type, "Metric type mismatch for " + enumValue.toString());
      assertEquals(entity.getUnit(), exp.unit, "Metric unit mismatch for " + enumValue.toString());
      assertEquals(entity.getDescription(), exp.description, "Description mismatch for " + enumValue.toString());
      assertEquals(entity.getDimensionsList(), exp.dimensions, "Dimensions mismatch for " + enumValue.toString());
    }
  }

  /** Expectation record for a single metric entity. */
  public static class MetricEntityExpectation {
    public final String name;
    public final MetricType type;
    public final MetricUnit unit;
    public final String description;
    public final Set<VeniceMetricsDimensions> dimensions;

    public MetricEntityExpectation(
        String name,
        MetricType type,
        MetricUnit unit,
        String description,
        Set<VeniceMetricsDimensions> dimensions) {
      this.name = name;
      this.type = type;
      this.unit = unit;
      this.description = description;
      this.dimensions = dimensions;
    }
  }
}
