package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class VeniceResponseStatusCategoryTest extends VeniceDimensionInterfaceTest<VeniceResponseStatusCategory> {
  protected VeniceResponseStatusCategoryTest() {
    super(VeniceResponseStatusCategory.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
  }

  @Override
  protected Map<VeniceResponseStatusCategory, String> expectedDimensionValueMapping() {
    return CollectionUtils.<VeniceResponseStatusCategory, String>mapBuilder()
        .put(VeniceResponseStatusCategory.SUCCESS, "success")
        .put(VeniceResponseStatusCategory.FAIL, "fail")
        .build();
  }
}
