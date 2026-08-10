package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceReplicationModeTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceReplicationMode, String> expectedValues = CollectionUtils.<VeniceReplicationMode, String>mapBuilder()
        .put(VeniceReplicationMode.NON_ACTIVE_ACTIVE, "non_active_active")
        .put(VeniceReplicationMode.ACTIVE_ACTIVE, "active_active")
        .build();
    new VeniceDimensionTestFixture<>(
        VeniceReplicationMode.class,
        VeniceMetricsDimensions.VENICE_REPLICATION_MODE,
        expectedValues).assertAll();
  }
}
