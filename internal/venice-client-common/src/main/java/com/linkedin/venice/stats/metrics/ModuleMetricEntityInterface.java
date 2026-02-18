package com.linkedin.venice.stats.metrics;

import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


/**
 * Every venice Module that defines its set of {@link MetricEntity} should implement this interface.
 */
public interface ModuleMetricEntityInterface {
  MetricEntity getMetricEntity();

  /**
   * Get all the unique {@link MetricEntity} from the provided enum classes based on the metric name.
   * This will also check if there are multiple metric entities with the same name across the provided
   * enum classes with different metric types and throw.
   */
  static Collection<MetricEntity> getUniqueMetricEntities(Class<? extends ModuleMetricEntityInterface>... enumClasses) {
    if (enumClasses == null || enumClasses.length == 0) {
      throw new IllegalArgumentException("Enum classes passed to getUniqueMetricEntities cannot be null or empty");
    }

    Map<String, MetricEntity> uniqueMetricsByName = new HashMap<>();
    for (Class<? extends ModuleMetricEntityInterface> enumClass: enumClasses) {
      ModuleMetricEntityInterface[] constants = enumClass.getEnumConstants();
      for (ModuleMetricEntityInterface constant: constants) {
        MetricEntity metric = constant.getMetricEntity();
        String metricName = metric.getMetricName();

        // Add only if not already present and validate if already present
        if (uniqueMetricsByName.containsKey(metricName)) {
          MetricEntity existingMetric = uniqueMetricsByName.get(metricName);
          if (!existingMetric.getMetricType().equals(metric.getMetricType())) {
            throw new IllegalArgumentException(
                "Multiple metric entities with the same name but different types found for metric : " + metricName
                    + " among the provided enum classes: " + Arrays.toString(enumClasses));
          }
        } else {
          uniqueMetricsByName.put(metricName, metric);
        }
      }
    }

    if (uniqueMetricsByName.isEmpty()) {
      throw new IllegalArgumentException("No metric entities found in the provided enum classes");
    }

    return uniqueMetricsByName.values();
  }

  @VisibleForTesting
  default String getMetricName() {
    return getMetricEntity().getMetricName();
  }
}
