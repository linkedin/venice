package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class StoreRepushTriggerSourceTest {
  @Test
  public void testDimensionInterface() {
    Map<StoreRepushTriggerSource, String> expectedValues =
        CollectionUtils.<StoreRepushTriggerSource, String>mapBuilder()
            .put(StoreRepushTriggerSource.MANUAL, "manual")
            .put(StoreRepushTriggerSource.SCHEDULED_FOR_LOG_COMPACTION, "scheduled_for_log_compaction")
            .build();
    new VeniceDimensionTestFixture<>(
        StoreRepushTriggerSource.class,
        VeniceMetricsDimensions.STORE_REPUSH_TRIGGER_SOURCE,
        expectedValues).assertAll();
  }
}
