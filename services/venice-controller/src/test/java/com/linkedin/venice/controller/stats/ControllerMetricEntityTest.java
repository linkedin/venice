package com.linkedin.venice.controller.stats;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.VeniceController;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import org.testng.annotations.Test;


/**
 * Validates that {@link VeniceController#CONTROLLER_SERVICE_METRIC_ENTITIES} is complete —
 * i.e., it aggregates metric entities from every {@link ModuleMetricEntityInterface} enum
 * defined in the controller stats package.
 *
 * <p>This test automatically discovers all {@link ModuleMetricEntityInterface} enums in
 * the {@code com.linkedin.venice.controller.stats} package by scanning the compiled
 * classes directories. If a new stats class adds a nested {@code ModuleMetricEntityInterface}
 * enum but does not register it in {@link VeniceController#CONTROLLER_SERVICE_METRIC_ENTITIES},
 * this test will fail.
 */
public class ControllerMetricEntityTest {
  private static final String STATS_PACKAGE = ControllerMetricEntityTest.class.getPackage().getName();

  @Test
  @SuppressWarnings("unchecked")
  public void testControllerServiceMetricEntitiesIsComplete() throws Exception {
    // Discover all ModuleMetricEntityInterface enums in the controller stats package.
    // Use getResources (plural) to enumerate all classpath locations for this package
    // (main classes and test classes are in separate directories).
    List<Class<? extends ModuleMetricEntityInterface>> discoveredEnumClasses = new ArrayList<>();

    String packagePath = STATS_PACKAGE.replace('.', '/');
    Enumeration<URL> packageUrls = Thread.currentThread().getContextClassLoader().getResources(packagePath);

    while (packageUrls.hasMoreElements()) {
      URL packageUrl = packageUrls.nextElement();
      File packageDir = new File(packageUrl.getFile());
      if (!packageDir.isDirectory()) {
        continue;
      }

      File[] classFiles = packageDir.listFiles();
      if (classFiles == null) {
        continue;
      }

      for (File classFile: classFiles) {
        String fileName = classFile.getName();
        // Nested classes compile to OuterClass$InnerClass.class
        if (!fileName.endsWith(".class") || !fileName.contains("$")) {
          continue;
        }
        String fullClassName = STATS_PACKAGE + "." + fileName.replace(".class", "");
        try {
          Class<?> clazz = Class.forName(fullClassName);
          if (clazz.isEnum() && ModuleMetricEntityInterface.class.isAssignableFrom(clazz)) {
            discoveredEnumClasses.add((Class<? extends ModuleMetricEntityInterface>) clazz);
          }
        } catch (ClassNotFoundException e) {
          // skip — not all nested class files may be loadable
        }
      }
    }

    assertTrue(
        !discoveredEnumClasses.isEmpty(),
        "No ModuleMetricEntityInterface enums found in " + STATS_PACKAGE + ". Classpath scanning may be broken.");

    // Build the expected set from all discovered enums
    Collection<MetricEntity> allExpected =
        ModuleMetricEntityInterface.getUniqueMetricEntities(discoveredEnumClasses.toArray(new Class[0]));

    Collection<MetricEntity> actual = VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;

    assertEquals(
        actual.size(),
        allExpected.size(),
        "CONTROLLER_SERVICE_METRIC_ENTITIES size mismatch. "
            + "A ModuleMetricEntityInterface enum in the controller stats package may not be registered in "
            + "VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES. Discovered enums: " + discoveredEnumClasses);

    for (MetricEntity expected: allExpected) {
      boolean found = false;
      for (MetricEntity entry: actual) {
        if (metricEntitiesEqual(entry, expected)) {
          found = true;
          break;
        }
      }
      assertTrue(found, "MetricEntity not found in CONTROLLER_SERVICE_METRIC_ENTITIES: " + expected.getMetricName());
    }
  }

  private boolean metricEntitiesEqual(MetricEntity a, MetricEntity b) {
    return Objects.equals(a.getMetricName(), b.getMetricName()) && a.getMetricType() == b.getMetricType()
        && a.getUnit() == b.getUnit() && Objects.equals(a.getDescription(), b.getDescription())
        && Objects.equals(a.getDimensionsList(), b.getDimensionsList());
  }
}
