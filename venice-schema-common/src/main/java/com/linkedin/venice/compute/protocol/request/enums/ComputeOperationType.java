package com.linkedin.venice.compute.protocol.request.enums;

import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.HashMap;
import java.util.Map;


public enum ComputeOperationType {
  DOT_PRODUCT(0);

  private final int value;
  private static final Map<Integer, ComputeOperationType> COMPUTE_OPERATION_TYPE_MAP = getOperationTypeMap();

  ComputeOperationType(int value) {
    this.value = value;
  }

  public static ComputeOperationType valueOf(int value) {
    ComputeOperationType type = COMPUTE_OPERATION_TYPE_MAP.get(value);
    if (type == null) {
      throw new VeniceException("Invalid compute operation type: " + value);
    }
    return type;
  }

  public static ComputeOperationType valueOf(ComputeOperation operation) {
    return valueOf(operation.operationType);
  }

  private static Map<Integer, ComputeOperationType> getOperationTypeMap() {
    Map<Integer, ComputeOperationType> intToTypeMap = new HashMap<>();
    for (ComputeOperationType type : ComputeOperationType.values()) {
      intToTypeMap.put(type.value, type);
    }
    return intToTypeMap;
  }
}
