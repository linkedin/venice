package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceStoreWriteTypeTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceStoreWriteType, String> expectedValues = CollectionUtils.<VeniceStoreWriteType, String>mapBuilder()
        .put(VeniceStoreWriteType.REGULAR_PUT, "regular_put")
        .put(VeniceStoreWriteType.PARTIAL_UPDATE, "partial_update")
        .build();
    new VeniceDimensionTestFixture<>(
        VeniceStoreWriteType.class,
        VeniceMetricsDimensions.VENICE_STORE_WRITE_TYPE,
        expectedValues).assertAll();
  }
}
