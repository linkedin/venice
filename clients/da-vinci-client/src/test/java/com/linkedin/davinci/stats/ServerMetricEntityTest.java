package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.davinci.stats.ServerMetricEntity.getMetricEntityEnumClasses;
import static com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.assertNoDuplicateMetricNamesAcrossEnums;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;


public class ServerMetricEntityTest {
  @Test
  public void testServerMetricEntitiesCount() {
    assertEquals(SERVER_METRIC_ENTITIES.size(), 142, "Expected 142 unique metric entities");
  }

  /**
   * Verifies that no two enum constants across all server metric entity enums share the same
   * metric name. Uses {@link ServerMetricEntity#getMetricEntityEnumClasses()} as the single
   * source of truth. Scans raw enum constants to catch silent deduplication by
   * {@link ModuleMetricEntityInterface#getUniqueMetricEntities}.
   */
  @Test
  public void testNoDuplicateMetricNamesAcrossServerEnums() {
    assertNoDuplicateMetricNamesAcrossEnums(getMetricEntityEnumClasses());
  }

  @Test
  public void testNoNullMetricEntities() {
    for (ModuleMetricEntityInterface[] enumValues: getAllModuleMetricEntityEnums()) {
      for (ModuleMetricEntityInterface metric: enumValues) {
        assertNotNull(
            metric.getMetricEntity(),
            "getMetricEntity() should not return null for " + ((Enum<?>) metric).name() + " in "
                + metric.getClass().getSimpleName());
      }
    }
  }

  /**
   * Scans the classpath for all enums implementing {@link ModuleMetricEntityInterface} in the
   * com.linkedin.davinci.stats package and verifies that every metric is registered in
   * SERVER_METRIC_ENTITIES. This catches cases where a new ModuleMetricEntityInterface enum
   * is added but not registered in ServerMetricEntity.
   */
  @Test
  public void testAllModuleMetricEntityEnumsAreRegisteredInServerMetricEntities() {
    Set<String> registeredMetricNames = new HashSet<>();
    for (MetricEntity entity: SERVER_METRIC_ENTITIES) {
      registeredMetricNames.add(entity.getMetricName());
    }

    for (ModuleMetricEntityInterface[] enumValues: getAllModuleMetricEntityEnums()) {
      for (ModuleMetricEntityInterface metric: enumValues) {
        String metricName = metric.getMetricEntity().getMetricName();
        assertTrue(
            registeredMetricNames.contains(metricName),
            "Metric '" + metricName + "' from " + metric.getClass().getSimpleName()
                + " is not registered in SERVER_METRIC_ENTITIES. "
                + "Add it to ServerMetricEntity.SERVER_METRIC_ENTITIES.");
      }
    }
  }

  /**
   * Scans the classpath for all enums implementing {@link ModuleMetricEntityInterface} in the
   * com.linkedin.davinci.stats package (including subpackages) and returns their enum constants.
   */
  private static List<ModuleMetricEntityInterface[]> getAllModuleMetricEntityEnums() {
    List<ModuleMetricEntityInterface[]> result = new ArrayList<>();
    try (ScanResult scanResult =
        new ClassGraph().whitelistPackages("com.linkedin.davinci.stats").enableClassInfo().scan()) {
      for (ClassInfo classInfo: scanResult.getClassesImplementing(ModuleMetricEntityInterface.class.getName())
          .filter(ClassInfo::isEnum)) {
        Class<?> enumClass = classInfo.loadClass();
        Object[] constants = enumClass.getEnumConstants();
        ModuleMetricEntityInterface[] metrics = new ModuleMetricEntityInterface[constants.length];
        for (int i = 0; i < constants.length; i++) {
          metrics[i] = (ModuleMetricEntityInterface) constants[i];
        }
        result.add(metrics);
      }
    }
    return result;
  }
}
