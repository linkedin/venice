package com.linkedin.venice.meta;

import com.linkedin.venice.utils.EnumUtils;
import com.linkedin.venice.utils.VeniceEnumValue;
import java.util.List;


/**
 * Integer wire codes for the current serving version's cache policy. Unrecognized codes bypass caching.
 */
public enum CurrentVersionType implements VeniceEnumValue {
  UNKNOWN(0), BATCH_ONLY(1), HYBRID(2);

  private static final List<CurrentVersionType> TYPES = EnumUtils.getEnumValuesList(CurrentVersionType.class);
  private final int value;

  CurrentVersionType(int value) {
    this.value = value;
  }

  public static CurrentVersionType valueOf(int value) {
    return value < 0 || value >= TYPES.size() ? UNKNOWN : TYPES.get(value);
  }

  @Override
  public int getValue() {
    return value;
  }
}
