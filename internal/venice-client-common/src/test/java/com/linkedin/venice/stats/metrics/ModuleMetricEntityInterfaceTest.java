package com.linkedin.venice.stats.metrics;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.testng.annotations.Test;


public class ModuleMetricEntityInterfaceTest {
  private static final Set<VeniceMetricsDimensions> TEST_DIMENSIONS = Collections.singleton(null);
  private static final String TEST_DESC = "test Description";

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Enum classes passed to getUniqueMetricEntities cannot be null or empty")
  public void testNullVarargs() {
    ModuleMetricEntityInterface.getUniqueMetricEntities((Class<? extends ModuleMetricEntityInterface>[]) null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Enum classes passed to getUniqueMetricEntities cannot be null or empty")
  public void testEmptyVarargs() {
    ModuleMetricEntityInterface.getUniqueMetricEntities();
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "No metric entities found in the provided enum classes")
  public void testNoMetricsFound() {
    ModuleMetricEntityInterface.getUniqueMetricEntities(EmptyEnum.class);
  }

  @Test
  public void testSingleEnum() {
    Collection<MetricEntity> metrics = ModuleMetricEntityInterface.getUniqueMetricEntities(SingleEnumForTest.class);

    assertEquals(metrics.size(), 1);
    MetricEntity m = metrics.iterator().next();
    assertEquals(m.getMetricName(), "enum_one");
    assertEquals(m.getMetricType(), MetricType.COUNTER);
    assertEquals(m.getUnit(), MetricUnit.NUMBER);
  }

  @Test
  public void testDistinctEnums() {
    Collection<MetricEntity> metrics = ModuleMetricEntityInterface.getUniqueMetricEntities(TwoDistinctEnum.class);

    assertEquals(metrics.size(), 2);
    Set<String> names = new HashSet<>();
    for (MetricEntity m: metrics) {
      names.add(m.getMetricName());
    }
    assertTrue(names.contains("enum_one"));
    assertTrue(names.contains("enum_two"));
  }

  /** Duplicate name and same type: should be deduped */
  @Test
  public void testDuplicateMetricsSameTypeAndUnit() {
    Collection<MetricEntity> metrics = ModuleMetricEntityInterface.getUniqueMetricEntities(DuplicateEnum.class);

    // only one unique name “enum_dup”
    assertEquals(metrics.size(), 1);

    // and it should be the instance from DuplicateEnum.ENUM_ONE by implementation
    MetricEntity m = metrics.iterator().next();
    assertTrue(m == DuplicateEnum.ENUM_ONE.getMetricEntity());
  }

  /** Duplicate name, different type: should throw */
  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Multiple metric entities with the same name but different types.*")
  public void testConflictMetricsDifferentUnit() {
    ModuleMetricEntityInterface.getUniqueMetricEntities(ConflictEnum.class);
  }

  private enum EmptyEnum implements ModuleMetricEntityInterface {
    ;
    @Override
    public MetricEntity getMetricEntity() {
      return null;
    }
  }

  public enum SingleEnumForTest implements ModuleMetricEntityInterface {
    ENUM_ONE(new MetricEntity("enum_one", MetricType.COUNTER, MetricUnit.NUMBER, TEST_DESC, TEST_DIMENSIONS));

    private final transient MetricEntity metric;

    SingleEnumForTest(MetricEntity m) {
      this.metric = m;
    }

    @Override
    public MetricEntity getMetricEntity() {
      return metric;
    }
  }

  private enum TwoDistinctEnum implements ModuleMetricEntityInterface {
    ENUM_ONE(new MetricEntity("enum_one", MetricType.COUNTER, MetricUnit.NUMBER, TEST_DESC, TEST_DIMENSIONS)),
    ENUM_TWO(new MetricEntity("enum_two", MetricType.HISTOGRAM, MetricUnit.MILLISECOND, TEST_DESC, TEST_DIMENSIONS));

    private final transient MetricEntity metric;

    TwoDistinctEnum(MetricEntity m) {
      this.metric = m;
    }

    @Override
    public MetricEntity getMetricEntity() {
      return metric;
    }
  }

  private enum DuplicateEnum implements ModuleMetricEntityInterface {
    ENUM_ONE(new MetricEntity("enum_dup", MetricType.COUNTER, MetricUnit.NUMBER, TEST_DESC, TEST_DIMENSIONS)),
    ENUM_TWO(new MetricEntity("enum_dup", MetricType.COUNTER, MetricUnit.NUMBER, TEST_DESC, TEST_DIMENSIONS));

    private final transient MetricEntity metric;

    DuplicateEnum(MetricEntity m) {
      this.metric = m;
    }

    @Override
    public MetricEntity getMetricEntity() {
      return metric;
    }
  }

  private enum ConflictEnum implements ModuleMetricEntityInterface {
    ENUM_CONFLICT_ONE(
        new MetricEntity("enum_conflict", MetricType.COUNTER, MetricUnit.NUMBER, TEST_DESC, TEST_DIMENSIONS)
    ),
    ENUM_CONFLICT_TWO(
        new MetricEntity("enum_conflict", MetricType.HISTOGRAM, MetricUnit.NUMBER, TEST_DESC, TEST_DIMENSIONS)
    );

    private final transient MetricEntity metric;

    ConflictEnum(MetricEntity m) {
      this.metric = m;
    }

    @Override
    public MetricEntity getMetricEntity() {
      return metric;
    }
  }
}
