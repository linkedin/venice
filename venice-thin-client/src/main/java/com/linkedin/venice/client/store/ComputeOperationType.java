package com.linkedin.venice.client.store;

import com.linkedin.venice.compute.protocol.request.DotProduct;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.HashMap;
import java.util.Map;


public enum ComputeOperationType {
  DOT_PRODUCT(0);

  private final int value;
  private static final Map<Integer, ComputeOperationType> OPERATION_TYPE_MAP = getOperationTypeMap();

  ComputeOperationType(int value) {
    this.value = value;
  }

  public Object getNewInstance() {
    switch (valueOf(value)) {
      case DOT_PRODUCT: return new DotProduct();
      default: throw new VeniceException("Unsupported " + getClass().getSimpleName() + " value: " + value);
    }
  }

  private static ComputeOperationType valueOf(int value) {
    ComputeOperationType type = OPERATION_TYPE_MAP.get(value);
    if (type == null) {
      throw new VeniceException("Invalid compute operation type: " + value);
    }
    return type;
  }

  private static Map<Integer, ComputeOperationType> getOperationTypeMap() {
    Map<Integer, ComputeOperationType> intToTypeMap = new HashMap<>();
    for (ComputeOperationType type : ComputeOperationType.values()) {
      intToTypeMap.put(type.value, type);
    }
    return intToTypeMap;
  }

  public int getValue() {
    return value;
  }
}
