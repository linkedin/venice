package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceDIVResultTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceDIVResult, String> expectedValues = CollectionUtils.<VeniceDIVResult, String>mapBuilder()
        .put(VeniceDIVResult.SUCCESS, "success")
        .put(VeniceDIVResult.DUPLICATE, "duplicate")
        .put(VeniceDIVResult.MISSING, "missing")
        .put(VeniceDIVResult.CORRUPTED, "corrupted")
        .build();
    new VeniceDimensionTestFixture<>(VeniceDIVResult.class, VeniceMetricsDimensions.VENICE_DIV_RESULT, expectedValues)
        .assertAll();
  }
}
