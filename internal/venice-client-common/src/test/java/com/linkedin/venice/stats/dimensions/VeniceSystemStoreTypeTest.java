package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class VeniceSystemStoreTypeTest extends VeniceDimensionInterfaceTest<VeniceSystemStoreType> {
  protected VeniceSystemStoreTypeTest() {
    super(VeniceSystemStoreType.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_SYSTEM_STORE_TYPE;
  }

  @Override
  protected Map<VeniceSystemStoreType, String> expectedDimensionValueMapping() {
    return CollectionUtils.<VeniceSystemStoreType, String>mapBuilder()
        .put(VeniceSystemStoreType.META_STORE, "meta_store")
        .put(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE, "davinci_push_status_store")
        .build();
  }
}
