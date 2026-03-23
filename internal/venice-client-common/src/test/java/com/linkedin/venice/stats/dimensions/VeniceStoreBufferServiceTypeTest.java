package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceStoreBufferServiceTypeTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceStoreBufferServiceType, String> expectedValues =
        CollectionUtils.<VeniceStoreBufferServiceType, String>mapBuilder()
            .put(VeniceStoreBufferServiceType.SORTED, "sorted")
            .put(VeniceStoreBufferServiceType.UNSORTED, "unsorted")
            .build();
    new VeniceDimensionTestFixture<>(
        VeniceStoreBufferServiceType.class,
        VeniceMetricsDimensions.VENICE_STORE_BUFFER_SERVICE_TYPE,
        expectedValues).assertAll();
  }
}
