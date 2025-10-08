package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.VeniceEnumValue;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


public enum VeniceETLStrategy implements VeniceEnumValue {
  /**
   * ETL is handled by external service completely.
   */
  EXTERNAL_SERVICE(1),
  /**
   * ETL is handled by external service, but Venice will trigger the creation and deletion of the ETL job(s).
   */
  EXTERNAL_WITH_VENICE_TRIGGER(2);

  private final int value;

  VeniceETLStrategy(int value) {
    this.value = value;
  }

  private static final Map<Integer, VeniceETLStrategy> idMapping = new HashMap<>();

  @Override
  public int getValue() {
    return value;
  }

  static {
    Arrays.stream(values()).forEach(s -> idMapping.put(s.value, s));
  }

  public static VeniceETLStrategy getVeniceETLStrategyFromInt(int value) {
    VeniceETLStrategy etlStrategy = idMapping.get(value);
    if (etlStrategy == null) {
      if (value == 0) {
        return EXTERNAL_SERVICE;
      }
      throw new VeniceException("Invalid VeniceETLStrategy id: " + value);
    }
    return etlStrategy;
  }
}
