package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class VeniceClientTypeTest extends VeniceDimensionInterfaceTest<VeniceClientType> {
  protected VeniceClientTypeTest() {
    super(VeniceClientType.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_CLIENT_TYPE;
  }

  @Override
  protected Map<VeniceClientType, String> expectedDimensionValueMapping() {
    return CollectionUtils.<VeniceClientType, String>mapBuilder()
        .put(VeniceClientType.THIN_CLIENT, "thin_client")
        .put(VeniceClientType.FAST_CLIENT, "fast_client")
        .put(VeniceClientType.DA_VINCI_CLIENT, "da_vinci_client")
        .build();
  }
}
