package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VenicePushJobDurationBucketTest {
  @Test
  public void testDimensionInterface() {
    Map<VenicePushJobDurationBucket, String> expectedValues =
        CollectionUtils.<VenicePushJobDurationBucket, String>mapBuilder()
            .put(VenicePushJobDurationBucket.UNDER_SLA, "under_sla")
            .put(VenicePushJobDurationBucket.AT_OR_OVER_SLA, "at_or_over_sla")
            .build();
    new VeniceDimensionTestFixture<>(
        VenicePushJobDurationBucket.class,
        VeniceMetricsDimensions.VENICE_PUSH_JOB_DURATION_BUCKET,
        expectedValues).assertAll();
  }
}
