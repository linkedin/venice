package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class VeniceRegionLocalityTest extends VeniceDimensionInterfaceTest<VeniceRegionLocality> {
  protected VeniceRegionLocalityTest() {
    super(VeniceRegionLocality.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_REGION_LOCALITY;
  }

  @Override
  protected Map<VeniceRegionLocality, String> expectedDimensionValueMapping() {
    return CollectionUtils.<VeniceRegionLocality, String>mapBuilder()
        .put(VeniceRegionLocality.LOCAL, "local")
        .put(VeniceRegionLocality.REMOTE, "remote")
        .build();
  }
}
