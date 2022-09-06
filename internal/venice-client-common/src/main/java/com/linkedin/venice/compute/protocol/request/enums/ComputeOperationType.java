package com.linkedin.venice.compute.protocol.request.enums;

import com.linkedin.venice.compute.CosineSimilarityOperator;
import com.linkedin.venice.compute.CountOperator;
import com.linkedin.venice.compute.DotProductOperator;
import com.linkedin.venice.compute.HadamardProductOperator;
import com.linkedin.venice.compute.ReadComputeOperator;
import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.CosineSimilarity;
import com.linkedin.venice.compute.protocol.request.Count;
import com.linkedin.venice.compute.protocol.request.DotProduct;
import com.linkedin.venice.compute.protocol.request.HadamardProduct;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.HashMap;
import java.util.Map;


public enum ComputeOperationType {
  DOT_PRODUCT(0, new DotProductOperator()), COSINE_SIMILARITY(1, new CosineSimilarityOperator()),
  HADAMARD_PRODUCT(2, new HadamardProductOperator()), COUNT(3, new CountOperator());

  private final ReadComputeOperator operator;
  private final int value;
  private static final Map<Integer, ComputeOperationType> OPERATION_TYPE_MAP = getOperationTypeMap();

  ComputeOperationType(int value, ReadComputeOperator operator) {
    this.value = value;
    this.operator = operator;
  }

  public Object getNewInstance() {
    switch (valueOf(value)) {
      case DOT_PRODUCT:
        return new DotProduct();
      case COSINE_SIMILARITY:
        return new CosineSimilarity();
      case HADAMARD_PRODUCT:
        return new HadamardProduct();
      case COUNT:
        return new Count();
      default:
        throw new VeniceException("Unsupported " + getClass().getSimpleName() + " value: " + value);
    }
  }

  private static ComputeOperationType valueOf(int value) {
    ComputeOperationType type = OPERATION_TYPE_MAP.get(value);
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
    for (ComputeOperationType type: ComputeOperationType.values()) {
      intToTypeMap.put(type.value, type);
    }
    return intToTypeMap;
  }

  public int getValue() {
    return value;
  }

  public ReadComputeOperator getOperator() {
    return operator;
  }
}
