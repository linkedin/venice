package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VenicePushJobSourceTest {
  @Test
  public void testDimensionInterface() {
    Map<VenicePushJobSource, String> expectedValues = CollectionUtils.<VenicePushJobSource, String>mapBuilder()
        .put(VenicePushJobSource.USER, "user")
        .put(VenicePushJobSource.REPUSH, "repush")
        .put(VenicePushJobSource.TTL_REPUSH, "ttl_repush")
        .put(VenicePushJobSource.ETL_REPUSH, "etl_repush")
        .put(VenicePushJobSource.COMPLIANCE, "compliance")
        .build();
    new VeniceDimensionTestFixture<>(
        VenicePushJobSource.class,
        VeniceMetricsDimensions.VENICE_PUSH_JOB_SOURCE,
        expectedValues).assertAll();
  }
}
