package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceOperationOutcomeTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceOperationOutcome, String> expectedValues = CollectionUtils.<VeniceOperationOutcome, String>mapBuilder()
        .put(VeniceOperationOutcome.SUCCESS, "success")
        .put(VeniceOperationOutcome.FAIL, "fail")
        .build();
    new VeniceDimensionTestFixture<>(
        VeniceOperationOutcome.class,
        VeniceMetricsDimensions.VENICE_OPERATION_OUTCOME,
        expectedValues).assertAll();
  }
}
