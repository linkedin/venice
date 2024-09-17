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
import com.linkedin.venice.utils.EnumUtils;
import com.linkedin.venice.utils.VeniceEnumValue;
import java.util.List;


public enum ComputeOperationType implements VeniceEnumValue {
  DOT_PRODUCT(0, new DotProductOperator()), COSINE_SIMILARITY(1, new CosineSimilarityOperator()),
  HADAMARD_PRODUCT(2, new HadamardProductOperator()), COUNT(3, new CountOperator());

  private final ReadComputeOperator operator;
  private final int value;
  private static final List<ComputeOperationType> TYPES = EnumUtils.getEnumValuesList(ComputeOperationType.class);

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

  public static ComputeOperationType valueOf(int value) {
    return EnumUtils.valueOf(TYPES, value, ComputeOperationType.class);
  }

  public static ComputeOperationType valueOf(ComputeOperation operation) {
    return valueOf(operation.operationType);
  }

  @Override
  public int getValue() {
    return value;
  }

  public ReadComputeOperator getOperator() {
    return operator;
  }
}
