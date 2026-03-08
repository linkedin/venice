package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class RequestTypeTest {
  @Test
  public void testDimensionInterface() {
    Map<RequestType, String> expectedValues = CollectionUtils.<RequestType, String>mapBuilder()
        .put(RequestType.SINGLE_GET, "single_get")
        .put(RequestType.MULTI_GET, "multi_get")
        .put(RequestType.MULTI_GET_STREAMING, "multi_get_streaming")
        .put(RequestType.COMPUTE, "compute")
        .put(RequestType.COMPUTE_STREAMING, "compute_streaming")
        .build();
    new VeniceDimensionTestFixture<>(RequestType.class, VeniceMetricsDimensions.VENICE_REQUEST_METHOD, expectedValues)
        .assertAll();
  }
}
