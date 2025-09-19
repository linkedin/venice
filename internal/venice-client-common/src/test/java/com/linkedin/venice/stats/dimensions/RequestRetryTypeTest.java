package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class RequestRetryTypeTest extends VeniceDimensionInterfaceTest<RequestRetryType> {
  protected RequestRetryTypeTest() {
    super(RequestRetryType.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_REQUEST_RETRY_TYPE;
  }

  @Override
  protected Map<RequestRetryType, String> expectedDimensionValueMapping() {
    return CollectionUtils.<RequestRetryType, String>mapBuilder()
        .put(RequestRetryType.ERROR_RETRY, "error_retry")
        .put(RequestRetryType.LONG_TAIL_RETRY, "long_tail_retry")
        .build();
  }
}
