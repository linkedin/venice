package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceProducerOperationTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceProducerOperation, String> expectedValues = CollectionUtils.<VeniceProducerOperation, String>mapBuilder()
        .put(VeniceProducerOperation.PUT, "put")
        .put(VeniceProducerOperation.DELETE, "delete")
        .put(VeniceProducerOperation.UPDATE, "update")
        .build();
    new VeniceDimensionTestFixture<>(
        VeniceProducerOperation.class,
        VeniceMetricsDimensions.VENICE_PRODUCER_OPERATION,
        expectedValues).assertAll();
  }
}
