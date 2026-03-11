package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceDCREventTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceDCREvent, String> expectedValues = CollectionUtils.<VeniceDCREvent, String>mapBuilder()
        .put(VeniceDCREvent.UPDATE_IGNORED, "update_ignored")
        .put(VeniceDCREvent.TOMBSTONE_CREATION, "tombstone_creation")
        .put(VeniceDCREvent.TIMESTAMP_REGRESSION_ERROR, "timestamp_regression_error")
        .put(VeniceDCREvent.OFFSET_REGRESSION_ERROR, "offset_regression_error")
        .build();
    new VeniceDimensionTestFixture<>(VeniceDCREvent.class, VeniceMetricsDimensions.VENICE_DCR_EVENT, expectedValues)
        .assertAll();
  }
}
