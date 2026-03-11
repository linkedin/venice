package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VenicePushJobStatusTest {
  @Test
  public void testDimensionInterface() {
    Map<VenicePushJobStatus, String> expectedValues = CollectionUtils.<VenicePushJobStatus, String>mapBuilder()
        .put(VenicePushJobStatus.SUCCESS, "success")
        .put(VenicePushJobStatus.USER_ERROR, "user_error")
        .put(VenicePushJobStatus.SYSTEM_ERROR, "system_error")
        .build();
    new VeniceDimensionTestFixture<>(
        VenicePushJobStatus.class,
        VeniceMetricsDimensions.VENICE_PUSH_JOB_STATUS,
        expectedValues).assertAll();
  }
}
