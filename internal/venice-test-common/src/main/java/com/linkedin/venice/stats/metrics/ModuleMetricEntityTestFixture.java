package com.linkedin.venice.stats.metrics;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


/**
 * Test fixture for validating enums implementing {@link ModuleMetricEntityInterface}. Create an
 * instance with the enum class and expected definitions, then call {@link #assertAll()} or
 * individual assertion methods. Registration in aggregated collections (e.g.,
 * {@code SERVER_METRIC_ENTITIES}) is validated by classpath-scanning tests like
 * {@code ServerMetricEntityTest}, not here.
 *
 * @param <T> the enum class under test
 */
public final class ModuleMetricEntityTestFixture<T extends Enum<T> & ModuleMetricEntityInterface> {
  private final Class<T> enumClass;
  private final Map<T, MetricEntityExpectation> expectedDefinitions;
  private final Map<String, Set<T>> allowedDuplicates;

  public ModuleMetricEntityTestFixture(Class<T> enumClass, Map<T, MetricEntityExpectation> expectedDefinitions) {
    this(enumClass, expectedDefinitions, Collections.emptyMap());
  }

  /**
   * @param allowedDuplicates maps each intentionally shared metric name to the exact set of enum
   *        constants that are permitted to share it. Any other constant reusing the same name will
   *        fail the duplicate check.
   */
  public ModuleMetricEntityTestFixture(
      Class<T> enumClass,
      Map<T, MetricEntityExpectation> expectedDefinitions,
      Map<String, Set<T>> allowedDuplicates) {
    this.enumClass = Objects.requireNonNull(enumClass);
    this.expectedDefinitions = Objects.requireNonNull(expectedDefinitions);
    this.allowedDuplicates = Objects.requireNonNull(allowedDuplicates, "allowedDuplicates");
  }

  /** Runs all assertions: count, duplicate names, and definitions. */
  public void assertAll() {
    assertMetricEntityCount();
    assertNoDuplicateMetricNames();
    assertMetricEntityDefinitions();
  }

  public void assertMetricEntityCount() {
    assertEquals(
        enumClass.getEnumConstants().length,
        expectedDefinitions.size(),
        "Enum value count does not match expected definitions. "
            + "Add or remove entries in expectedDefinitions when modifying " + enumClass.getSimpleName());
  }

  public void assertNoDuplicateMetricNames() {
    // Build a map of metric name -> set of enum constants that use it
    Map<String, Set<T>> nameToConstants = new HashMap<>();
    for (T enumValue: enumClass.getEnumConstants()) {
      String name = enumValue.getMetricEntity().getMetricName();
      nameToConstants.computeIfAbsent(name, k -> new HashSet<>()).add(enumValue);
    }

    for (Map.Entry<String, Set<T>> entry: nameToConstants.entrySet()) {
      String name = entry.getKey();
      Set<T> constants = entry.getValue();
      if (constants.size() <= 1) {
        continue;
      }
      Set<T> allowed = allowedDuplicates.get(name);
      assertNotNull(
          allowed,
          "Unexpected duplicate metric name '" + name + "' in " + enumClass.getSimpleName() + " shared by constants "
              + constants + ". If intentional, add an entry to allowedDuplicates.");
      assertEquals(
          constants,
          allowed,
          "Metric name '" + name + "' in " + enumClass.getSimpleName()
              + " is shared by a different set of constants than allowed.");
    }

    // Verify no stale entries in allowedDuplicates
    for (Map.Entry<String, Set<T>> entry: allowedDuplicates.entrySet()) {
      String name = entry.getKey();
      Set<T> constants = nameToConstants.get(name);
      assertTrue(
          constants != null && constants.size() > 1,
          "Stale entry in allowedDuplicates: metric name '" + name + "' in " + enumClass.getSimpleName()
              + " is not actually duplicated. Remove it from allowedDuplicates.");
    }
  }

  public void assertMetricEntityDefinitions() {
    for (T enumValue: enumClass.getEnumConstants()) {
      MetricEntityExpectation exp = expectedDefinitions.get(enumValue);
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

  /**
   * Asserts that no two enum constants across the given {@link ModuleMetricEntityInterface} enum
   * classes share the same metric name. Unlike checking the output of
   * {@link ModuleMetricEntityInterface#getUniqueMetricEntities}, this scans the raw enum constants
   * and catches silent deduplication.
   *
   * @param allowedDuplicateNames metric names that are intentionally shared (e.g., when TC and
   *        DVC variants of a metric share the same OTel name). Any duplicate not in this set
   *        will fail the assertion.
   */
  public static void assertNoDuplicateMetricNamesAcrossEnums(
      Set<String> allowedDuplicateNames,
      List<Class<? extends ModuleMetricEntityInterface>> enumClasses) {
    Map<String, String> nameToSource = new HashMap<>();
    for (Class<? extends ModuleMetricEntityInterface> enumClass: enumClasses) {
      for (ModuleMetricEntityInterface constant: enumClass.getEnumConstants()) {
        String name = constant.getMetricEntity().getMetricName();
        String source = enumClass.getSimpleName() + "." + ((Enum<?>) constant).name();
        String existing = nameToSource.put(name, source);
        if (existing != null && !allowedDuplicateNames.contains(name)) {
          throw new AssertionError(
              "Duplicate metric name '" + name + "' found across enums: " + existing + " and " + source
                  + ". This would be silently deduplicated by getUniqueMetricEntities()."
                  + " If intentional, add it to allowedDuplicateNames.");
        }
      }
    }
  }

  /** Convenience overload with no allowed duplicates. */
  public static void assertNoDuplicateMetricNamesAcrossEnums(
      List<Class<? extends ModuleMetricEntityInterface>> enumClasses) {
    assertNoDuplicateMetricNamesAcrossEnums(Collections.emptySet(), enumClasses);
  }

  /** Structural equality check for {@link MetricEntity} instances (no equals/hashCode on MetricEntity). */
  public static boolean metricEntitiesEqual(MetricEntity a, MetricEntity b) {
    return Objects.equals(a.getMetricName(), b.getMetricName()) && a.getMetricType() == b.getMetricType()
        && a.getUnit() == b.getUnit() && Objects.equals(a.getDescription(), b.getDescription())
        && Objects.equals(a.getDimensionsList(), b.getDimensionsList());
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
      this.name = Objects.requireNonNull(name, "name");
      this.type = Objects.requireNonNull(type, "type");
      this.unit = Objects.requireNonNull(unit, "unit");
      this.description = Objects.requireNonNull(description, "description");
      this.dimensions = dimensions != null ? dimensions : Collections.emptySet();
    }
  }
}
