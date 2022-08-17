package com.linkedin.venice.common;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.systemstore.schemas.CurrentStoreStates;
import com.linkedin.venice.meta.systemstore.schemas.CurrentVersionStates;
import com.linkedin.venice.meta.systemstore.schemas.StoreAttributes;
import com.linkedin.venice.meta.systemstore.schemas.StoreKeySchemas;
import com.linkedin.venice.meta.systemstore.schemas.StoreValueSchemas;
import com.linkedin.venice.meta.systemstore.schemas.TargetVersionStates;
import java.util.HashMap;
import java.util.Map;


public enum StoreMetadataType {
  STORE_ATTRIBUTES(1), TARGET_VERSION_STATES(2), CURRENT_STORE_STATES(3), CURRENT_VERSION_STATES(4),
  STORE_KEY_SCHEMAS(5), STORE_VALUE_SCHEMAS(6);

  private final int value;
  private static final Map<Integer, StoreMetadataType> METADATA_TYPE_MAP = getMetadataTypeMap();

  StoreMetadataType(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public Object getNewInstance() {
    switch (valueOf(value)) {
      case STORE_ATTRIBUTES:
        return new StoreAttributes();
      case TARGET_VERSION_STATES:
        return new TargetVersionStates();
      case CURRENT_STORE_STATES:
        return new CurrentStoreStates();
      case CURRENT_VERSION_STATES:
        return new CurrentVersionStates();
      case STORE_KEY_SCHEMAS:
        return new StoreKeySchemas();
      case STORE_VALUE_SCHEMAS:
        return new StoreValueSchemas();
      default:
        throw new VeniceException("Unsupported " + getClass().getSimpleName() + " value: " + value);
    }
  }

  private static Map<Integer, StoreMetadataType> getMetadataTypeMap() {
    Map<Integer, StoreMetadataType> map = new HashMap<>();
    for (StoreMetadataType type: StoreMetadataType.values()) {
      map.put(type.value, type);
    }
    return map;
  }

  private static StoreMetadataType valueOf(int value) {
    StoreMetadataType type = METADATA_TYPE_MAP.get(value);
    if (type == null) {
      throw new VeniceException("Unsupported " + StoreMetadataType.class.getSimpleName() + " value: " + value);
    }
    return type;
  }
}
