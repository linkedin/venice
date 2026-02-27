package com.linkedin.venice.controller.stats;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.VeniceController;
import com.linkedin.venice.stats.ThreadPoolOtelMetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import org.testng.annotations.Test;


/**
 * Validates that {@link VeniceController#CONTROLLER_SERVICE_METRIC_ENTITIES} is complete —
 * i.e., it aggregates metric entities from every {@link ModuleMetricEntityInterface} enum
 * registered by the controller.
 *
 * <p>This test automatically discovers all {@link ModuleMetricEntityInterface} enums
 * (both nested and top-level) in the controller packages by scanning the compiled classes
 * directories. It also includes enums from shared modules (e.g., {@link ThreadPoolOtelMetricEntity})
 * that are registered by the controller. If a new {@code ModuleMetricEntityInterface} enum is added
 * but not registered in {@link VeniceController#CONTROLLER_SERVICE_METRIC_ENTITIES}, this test will
 * fail.
 */
public class ControllerMetricEntityTest {
  /**
   * Controller packages to scan for ModuleMetricEntityInterface enums.
   */
  private static final List<String> CONTROLLER_PACKAGES =
      Arrays.asList("com.linkedin.venice.controller.stats", "com.linkedin.venice.controller.lingeringjob");

  /**
   * ModuleMetricEntityInterface enums from shared modules (outside controller packages)
   * that are registered in CONTROLLER_SERVICE_METRIC_ENTITIES.
   */
  private static final List<Class<? extends ModuleMetricEntityInterface>> SHARED_MODULE_ENUMS =
      Arrays.asList(ThreadPoolOtelMetricEntity.class);

  @Test
  @SuppressWarnings("unchecked")
  public void testControllerServiceMetricEntitiesIsComplete() throws Exception {
    // Discover all ModuleMetricEntityInterface enums in the controller packages.
    List<Class<? extends ModuleMetricEntityInterface>> discoveredEnumClasses = new ArrayList<>();

    for (String packageName: CONTROLLER_PACKAGES) {
      discoverEnumsInPackage(packageName, discoveredEnumClasses);
    }

    // Add shared module enums
    discoveredEnumClasses.addAll(SHARED_MODULE_ENUMS);

    assertTrue(
        !discoveredEnumClasses.isEmpty(),
        "No ModuleMetricEntityInterface enums found. Classpath scanning may be broken.");

    // Build the expected set from all discovered enums
    Collection<MetricEntity> allExpected =
        ModuleMetricEntityInterface.getUniqueMetricEntities(discoveredEnumClasses.toArray(new Class[0]));

    Collection<MetricEntity> actual = VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;

    assertEquals(
        actual.size(),
        allExpected.size(),
        "CONTROLLER_SERVICE_METRIC_ENTITIES size mismatch. "
            + "A ModuleMetricEntityInterface enum in the controller packages may not be registered in "
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

  @SuppressWarnings("unchecked")
  private void discoverEnumsInPackage(String packageName, List<Class<? extends ModuleMetricEntityInterface>> result)
      throws Exception {
    String packagePath = packageName.replace('.', '/');
    Enumeration<URL> packageUrls = Thread.currentThread().getContextClassLoader().getResources(packagePath);

    while (packageUrls.hasMoreElements()) {
      URL packageUrl = packageUrls.nextElement();
      if (!"file".equals(packageUrl.getProtocol())) {
        continue;
      }
      File packageDir;
      try {
        packageDir = Paths.get(packageUrl.toURI()).toFile();
      } catch (URISyntaxException e) {
        continue;
      }
      if (!packageDir.isDirectory()) {
        continue;
      }

      File[] classFiles = packageDir.listFiles();
      if (classFiles == null) {
        continue;
      }

      for (File classFile: classFiles) {
        String fileName = classFile.getName();
        if (!fileName.endsWith(".class")) {
          continue;
        }
        String fullClassName = packageName + "." + fileName.replace(".class", "");
        try {
          Class<?> clazz = Class.forName(fullClassName);
          if (clazz.isEnum() && ModuleMetricEntityInterface.class.isAssignableFrom(clazz)) {
            result.add((Class<? extends ModuleMetricEntityInterface>) clazz);
          }
        } catch (ClassNotFoundException e) {
          // skip — not all nested class files may be loadable
        }
      }
    }
  }

  private boolean metricEntitiesEqual(MetricEntity a, MetricEntity b) {
    return Objects.equals(a.getMetricName(), b.getMetricName()) && a.getMetricType() == b.getMetricType()
        && a.getUnit() == b.getUnit() && Objects.equals(a.getDescription(), b.getDescription())
        && Objects.equals(a.getDimensionsList(), b.getDimensionsList());
  }
}
