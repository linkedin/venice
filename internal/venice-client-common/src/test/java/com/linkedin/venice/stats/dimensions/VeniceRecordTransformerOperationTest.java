package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceRecordTransformerOperationTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceRecordTransformerOperation, String> expectedValues =
        CollectionUtils.<VeniceRecordTransformerOperation, String>mapBuilder()
            .put(VeniceRecordTransformerOperation.PUT, "put")
            .put(VeniceRecordTransformerOperation.DELETE, "delete")
            .build();
    new VeniceDimensionTestFixture<>(
        VeniceRecordTransformerOperation.class,
        VeniceMetricsDimensions.VENICE_RECORD_TRANSFORMER_OPERATION,
        expectedValues).assertAll();
  }
}
