package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceConsumerPoolActionTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceConsumerPoolAction, String> expectedValues =
        CollectionUtils.<VeniceConsumerPoolAction, String>mapBuilder()
            .put(VeniceConsumerPoolAction.SUBSCRIBE, "subscribe")
            .put(VeniceConsumerPoolAction.UPDATE_ASSIGNMENT, "update_assignment")
            .build();
    new VeniceDimensionTestFixture<>(
        VeniceConsumerPoolAction.class,
        VeniceMetricsDimensions.VENICE_CONSUMER_POOL_ACTION,
        expectedValues).assertAll();
  }
}
