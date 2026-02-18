package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class VenicePushJobStatusTest extends VeniceDimensionInterfaceTest<VenicePushJobStatus> {
  protected VenicePushJobStatusTest() {
    super(VenicePushJobStatus.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_PUSH_JOB_STATUS;
  }

  @Override
  protected Map<VenicePushJobStatus, String> expectedDimensionValueMapping() {
    return CollectionUtils.<VenicePushJobStatus, String>mapBuilder()
        .put(VenicePushJobStatus.SUCCESS, "success")
        .put(VenicePushJobStatus.USER_ERROR, "user_error")
        .put(VenicePushJobStatus.SYSTEM_ERROR, "system_error")
        .build();
  }
}
