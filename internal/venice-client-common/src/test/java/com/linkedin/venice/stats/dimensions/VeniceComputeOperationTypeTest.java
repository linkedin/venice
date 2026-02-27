package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class VeniceComputeOperationTypeTest extends VeniceDimensionInterfaceTest<VeniceComputeOperationType> {
  protected VeniceComputeOperationTypeTest() {
    super(VeniceComputeOperationType.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_COMPUTE_OPERATION_TYPE;
  }

  @Override
  protected Map<VeniceComputeOperationType, String> expectedDimensionValueMapping() {
    return CollectionUtils.<VeniceComputeOperationType, String>mapBuilder()
        .put(VeniceComputeOperationType.DOT_PRODUCT, "dot_product")
        .put(VeniceComputeOperationType.COSINE_SIMILARITY, "cosine_similarity")
        .put(VeniceComputeOperationType.HADAMARD_PRODUCT, "hadamard_product")
        .put(VeniceComputeOperationType.COUNT, "count")

        .build();
  }
}
