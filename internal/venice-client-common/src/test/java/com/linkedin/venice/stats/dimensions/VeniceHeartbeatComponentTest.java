package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceHeartbeatComponentTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceHeartbeatComponent, String> expectedValues =
        CollectionUtils.<VeniceHeartbeatComponent, String>mapBuilder()
            .put(VeniceHeartbeatComponent.REPORTER, "reporter")
            .put(VeniceHeartbeatComponent.LOGGER, "logger")
            .build();
    new VeniceDimensionTestFixture<>(
        VeniceHeartbeatComponent.class,
        VeniceMetricsDimensions.VENICE_HEARTBEAT_COMPONENT,
        expectedValues).assertAll();
  }
}
