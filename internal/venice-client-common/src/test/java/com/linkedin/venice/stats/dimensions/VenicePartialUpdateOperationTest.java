package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class VenicePartialUpdateOperationTest extends VeniceDimensionInterfaceTest<VenicePartialUpdateOperation> {
  protected VenicePartialUpdateOperationTest() {
    super(VenicePartialUpdateOperation.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_PARTIAL_UPDATE_OPERATION_PHASE;
  }

  @Override
  protected Map<VenicePartialUpdateOperation, String> expectedDimensionValueMapping() {
    return CollectionUtils.<VenicePartialUpdateOperation, String>mapBuilder()
        .put(VenicePartialUpdateOperation.QUERY, "query")
        .put(VenicePartialUpdateOperation.UPDATE, "update")
        .build();
  }
}
