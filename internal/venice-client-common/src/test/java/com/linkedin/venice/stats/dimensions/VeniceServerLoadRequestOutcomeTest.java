package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceServerLoadRequestOutcomeTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceServerLoadRequestOutcome, String> expectedValues =
        CollectionUtils.<VeniceServerLoadRequestOutcome, String>mapBuilder()
            .put(VeniceServerLoadRequestOutcome.ACCEPTED, "accepted")
            .put(VeniceServerLoadRequestOutcome.REJECTED, "rejected")
            .build();
    new VeniceDimensionTestFixture<>(
        VeniceServerLoadRequestOutcome.class,
        VeniceMetricsDimensions.VENICE_SERVER_LOAD_REQUEST_OUTCOME,
        expectedValues).assertAll();
  }
}
