package com.linkedin.venice.controller.helix;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.Validate;


public final class HelixCapacityConfig {
  private final List<String> helixInstanceCapacityKeys;
  private final Map<String, Integer> helixDefaultInstanceCapacityMap;
  private final Map<String, Integer> helixDefaultPartitionWeightMap;

  public HelixCapacityConfig(
      List<String> helixInstanceCapacityKeys,
      Map<String, Integer> helixDefaultInstanceCapacityMap,
      Map<String, Integer> helixDefaultPartitionWeightMap) {
    Validate.notNull(helixInstanceCapacityKeys, "Instance capacity keys cannot be null");
    Validate.notNull(helixDefaultInstanceCapacityMap, "Default instance capacity map cannot be null");
    Validate.notNull(helixDefaultPartitionWeightMap, "Default partition weight map cannot be null");
    this.helixInstanceCapacityKeys = helixInstanceCapacityKeys;
    this.helixDefaultInstanceCapacityMap = helixDefaultInstanceCapacityMap;
    this.helixDefaultPartitionWeightMap = helixDefaultPartitionWeightMap;
  }

  public List<String> getHelixInstanceCapacityKeys() {
    return helixInstanceCapacityKeys;
  }

  public Map<String, Integer> getHelixDefaultInstanceCapacityMap() {
    return helixDefaultInstanceCapacityMap;
  }

  public Map<String, Integer> getHelixDefaultPartitionWeightMap() {
    return helixDefaultPartitionWeightMap;
  }
}
