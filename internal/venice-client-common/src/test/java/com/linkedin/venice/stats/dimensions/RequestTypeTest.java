package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class RequestTypeTest extends VeniceDimensionInterfaceTest<RequestType> {
  protected RequestTypeTest() {
    super(RequestType.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
  }

  @Override
  protected Map<RequestType, String> expectedDimensionValueMapping() {
    return CollectionUtils.<RequestType, String>mapBuilder()
        .put(RequestType.SINGLE_GET, "single_get")
        .put(RequestType.MULTI_GET, "multi_get")
        .put(RequestType.MULTI_GET_STREAMING, "multi_get_streaming")
        .put(RequestType.COMPUTE, "compute")
        .put(RequestType.COMPUTE_STREAMING, "compute_streaming")
        .build();
  }
}
