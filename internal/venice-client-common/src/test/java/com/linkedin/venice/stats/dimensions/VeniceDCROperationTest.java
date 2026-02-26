package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class VeniceDCROperationTest extends VeniceDimensionInterfaceTest<VeniceDCROperation> {
  protected VeniceDCROperationTest() {
    super(VeniceDCROperation.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_DCR_OPERATION;
  }

  @Override
  protected Map<VeniceDCROperation, String> expectedDimensionValueMapping() {
    return CollectionUtils.<VeniceDCROperation, String>mapBuilder()
        .put(VeniceDCROperation.PUT, "put")
        .put(VeniceDCROperation.UPDATE, "update")
        .put(VeniceDCROperation.DELETE, "delete")
        .build();
  }
}
