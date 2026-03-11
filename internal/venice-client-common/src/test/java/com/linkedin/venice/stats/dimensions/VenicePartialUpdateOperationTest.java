package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VenicePartialUpdateOperationTest {
  @Test
  public void testDimensionInterface() {
    Map<VenicePartialUpdateOperation, String> expectedValues =
        CollectionUtils.<VenicePartialUpdateOperation, String>mapBuilder()
            .put(VenicePartialUpdateOperation.QUERY, "query")
            .put(VenicePartialUpdateOperation.UPDATE, "update")
            .build();
    new VeniceDimensionTestFixture<>(
        VenicePartialUpdateOperation.class,
        VeniceMetricsDimensions.VENICE_PARTIAL_UPDATE_OPERATION_PHASE,
        expectedValues).assertAll();
  }
}
