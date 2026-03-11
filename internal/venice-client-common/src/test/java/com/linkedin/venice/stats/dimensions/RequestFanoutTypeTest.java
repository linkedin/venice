package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class RequestFanoutTypeTest {
  @Test
  public void testDimensionInterface() {
    Map<RequestFanoutType, String> expectedValues = CollectionUtils.<RequestFanoutType, String>mapBuilder()
        .put(RequestFanoutType.ORIGINAL, "original")
        .put(RequestFanoutType.RETRY, "retry")
        .build();
    new VeniceDimensionTestFixture<>(
        RequestFanoutType.class,
        VeniceMetricsDimensions.VENICE_REQUEST_FANOUT_TYPE,
        expectedValues).assertAll();
  }
}
