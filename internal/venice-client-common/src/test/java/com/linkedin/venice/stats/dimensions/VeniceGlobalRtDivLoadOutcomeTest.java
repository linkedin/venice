package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceGlobalRtDivLoadOutcomeTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceGlobalRtDivLoadOutcome, String> expectedValues =
        CollectionUtils.<VeniceGlobalRtDivLoadOutcome, String>mapBuilder()
            .put(VeniceGlobalRtDivLoadOutcome.FOUND, "found")
            .put(VeniceGlobalRtDivLoadOutcome.NOT_FOUND, "not_found")
            .build();
    new VeniceDimensionTestFixture<>(
        VeniceGlobalRtDivLoadOutcome.class,
        VeniceMetricsDimensions.VENICE_GLOBAL_RT_DIV_LOAD_OUTCOME,
        expectedValues).assertAll();
  }
}
