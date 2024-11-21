package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.metrics.modules.RouterMetricEntities;
import java.util.HashMap;
import java.util.Map;


public class AllMetricEntities {
  private static final Map<String, Class<? extends Enum<?>>> allModuleMetricEntitiesEnums = new HashMap<>();

  // Add all the components metric enum classes
  static {
    allModuleMetricEntitiesEnums.put("venice.router", RouterMetricEntities.class);
  }

  // Method to retrieve an enum class by key
  public static Class<? extends Enum<?>> getModuleMetricEntityEnum(String key) {
    return allModuleMetricEntitiesEnums.get(key);
  }
}
