package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class InstanceErrorTypeTest {
  @Test
  public void testDimensionInterface() {
    Map<InstanceErrorType, String> expectedValues = CollectionUtils.<InstanceErrorType, String>mapBuilder()
        .put(InstanceErrorType.BLOCKED, "blocked")
        .put(InstanceErrorType.UNHEALTHY, "unhealthy")
        .put(InstanceErrorType.OVERLOADED, "overloaded")
        .build();
    new VeniceDimensionTestFixture<>(
        InstanceErrorType.class,
        VeniceMetricsDimensions.VENICE_INSTANCE_ERROR_TYPE,
        expectedValues).assertAll();
  }
}
