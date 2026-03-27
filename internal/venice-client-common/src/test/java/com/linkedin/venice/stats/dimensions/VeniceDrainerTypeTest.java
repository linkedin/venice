package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceDrainerTypeTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceDrainerType, String> expectedValues = CollectionUtils.<VeniceDrainerType, String>mapBuilder()
        .put(VeniceDrainerType.SORTED, "sorted")
        .put(VeniceDrainerType.UNSORTED, "unsorted")
        .build();
    new VeniceDimensionTestFixture<>(
        VeniceDrainerType.class,
        VeniceMetricsDimensions.VENICE_DRAINER_TYPE,
        expectedValues).assertAll();
  }
}
