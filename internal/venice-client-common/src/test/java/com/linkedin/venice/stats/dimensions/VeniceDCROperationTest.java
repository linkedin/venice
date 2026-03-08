package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceDCROperationTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceDCROperation, String> expectedValues = CollectionUtils.<VeniceDCROperation, String>mapBuilder()
        .put(VeniceDCROperation.PUT, "put")
        .put(VeniceDCROperation.UPDATE, "update")
        .put(VeniceDCROperation.DELETE, "delete")
        .build();
    new VeniceDimensionTestFixture<>(
        VeniceDCROperation.class,
        VeniceMetricsDimensions.VENICE_DCR_OPERATION,
        expectedValues).assertAll();
  }
}
