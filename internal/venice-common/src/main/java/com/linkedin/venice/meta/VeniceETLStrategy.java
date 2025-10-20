package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.VeniceEnumValue;


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

  @Override
  public int getValue() {
    return value;
  }

  public static VeniceETLStrategy getVeniceETLStrategyFromInt(int value) {
    if (value == 0) {
      // Normalize legacy/default value to canonical default
      return EXTERNAL_SERVICE;
    }
    for (VeniceETLStrategy strategy: VeniceETLStrategy.values()) {
      if (strategy.getValue() == value) {
        return strategy;
      }
    }
    throw new VeniceException("Invalid VeniceETLStrategy id: " + value);
  }
}
