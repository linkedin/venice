package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
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
    assertEquals(SERVER_METRIC_ENTITIES.size(), 56, "Expected 56 unique metric entities");
  }

  @Test
  public void testNoDuplicateMetricNames() {
    Set<String> allNames = new HashSet<>();
    for (ModuleMetricEntityInterface[] enumValues: getAllModuleMetricEntityEnums()) {
      for (ModuleMetricEntityInterface metric: enumValues) {
        String name = metric.getMetricEntity().getMetricName();
        assertTrue(allNames.add(name), "Duplicate metric name found: " + name);
      }
    }
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
