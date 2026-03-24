package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceConnectionSourceTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceConnectionSource, String> expectedValues = CollectionUtils.<VeniceConnectionSource, String>mapBuilder()
        .put(VeniceConnectionSource.ROUTER, "router")
        .put(VeniceConnectionSource.CLIENT, "client")
        .build();
    new VeniceDimensionTestFixture<>(
        VeniceConnectionSource.class,
        VeniceMetricsDimensions.VENICE_CONNECTION_SOURCE,
        expectedValues).assertAll();
  }
}
