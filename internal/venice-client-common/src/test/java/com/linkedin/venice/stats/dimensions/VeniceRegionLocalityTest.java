package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceRegionLocalityTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceRegionLocality, String> expectedValues = CollectionUtils.<VeniceRegionLocality, String>mapBuilder()
        .put(VeniceRegionLocality.LOCAL, "local")
        .put(VeniceRegionLocality.REMOTE, "remote")
        .build();
    new VeniceDimensionTestFixture<>(
        VeniceRegionLocality.class,
        VeniceMetricsDimensions.VENICE_REGION_LOCALITY,
        expectedValues).assertAll();
  }
}
