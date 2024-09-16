package com.linkedin.venice.compute.protocol.request.enums;

import com.linkedin.alpini.base.misc.CollectionUtil;
import com.linkedin.venice.utils.VeniceEnumValueTest;
import java.util.Map;


public class ComputeOperationTypeTest extends VeniceEnumValueTest<ComputeOperationType> {
  public ComputeOperationTypeTest() {
    super(ComputeOperationType.class);
  }

  @Override
  protected Map<Integer, ComputeOperationType> expectedMapping() {
    return CollectionUtil.<Integer, ComputeOperationType>mapBuilder()
        .put(0, ComputeOperationType.DOT_PRODUCT)
        .put(1, ComputeOperationType.COSINE_SIMILARITY)
        .put(2, ComputeOperationType.HADAMARD_PRODUCT)
        .put(3, ComputeOperationType.COUNT)
        .build();
  }
}
