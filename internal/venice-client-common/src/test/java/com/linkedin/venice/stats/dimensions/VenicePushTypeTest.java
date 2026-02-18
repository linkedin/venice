package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class VenicePushTypeTest extends VeniceDimensionInterfaceTest<VenicePushType> {
  protected VenicePushTypeTest() {
    super(VenicePushType.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_PUSH_JOB_TYPE;
  }

  @Override
  protected Map<VenicePushType, String> expectedDimensionValueMapping() {
    return CollectionUtils.<VenicePushType, String>mapBuilder()
        .put(VenicePushType.BATCH, "batch")
        .put(VenicePushType.INCREMENTAL, "incremental")
        .build();
  }
}
