package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceDIVSeverityTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceDIVSeverity, String> expectedValues = CollectionUtils.<VeniceDIVSeverity, String>mapBuilder()
        .put(VeniceDIVSeverity.BENIGN, "benign")
        .put(VeniceDIVSeverity.POTENTIALLY_LOSSY, "potentially_lossy")
        .build();
    new VeniceDimensionTestFixture<>(
        VeniceDIVSeverity.class,
        VeniceMetricsDimensions.VENICE_DIV_SEVERITY,
        expectedValues).assertAll();
  }
}
