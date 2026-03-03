package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class VeniceWriteComputeOperationTest extends VeniceDimensionInterfaceTest<VeniceWriteComputeOperation> {
  protected VeniceWriteComputeOperationTest() {
    super(VeniceWriteComputeOperation.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_WRITE_COMPUTE_OPERATION;
  }

  @Override
  protected Map<VeniceWriteComputeOperation, String> expectedDimensionValueMapping() {
    return CollectionUtils.<VeniceWriteComputeOperation, String>mapBuilder()
        .put(VeniceWriteComputeOperation.QUERY, "query")
        .put(VeniceWriteComputeOperation.UPDATE, "update")
        .build();
  }
}
