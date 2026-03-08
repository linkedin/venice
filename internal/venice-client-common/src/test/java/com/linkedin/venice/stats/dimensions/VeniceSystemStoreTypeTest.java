package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceSystemStoreTypeTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceSystemStoreType, String> expectedValues = CollectionUtils.<VeniceSystemStoreType, String>mapBuilder()
        .put(VeniceSystemStoreType.META_STORE, "meta_store")
        .put(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE, "davinci_push_status_store")
        .build();
    new VeniceDimensionTestFixture<>(
        VeniceSystemStoreType.class,
        VeniceMetricsDimensions.VENICE_SYSTEM_STORE_TYPE,
        expectedValues).assertAll();
  }
}
