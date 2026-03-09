package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class RequestRetryTypeTest {
  @Test
  public void testDimensionInterface() {
    Map<RequestRetryType, String> expectedValues = CollectionUtils.<RequestRetryType, String>mapBuilder()
        .put(RequestRetryType.ERROR_RETRY, "error_retry")
        .put(RequestRetryType.LONG_TAIL_RETRY, "long_tail_retry")
        .build();
    new VeniceDimensionTestFixture<>(
        RequestRetryType.class,
        VeniceMetricsDimensions.VENICE_REQUEST_RETRY_TYPE,
        expectedValues).assertAll();
  }
}
