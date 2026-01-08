package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class VersionTypeTest extends VeniceDimensionInterfaceTest<VersionType> {
  protected VersionTypeTest() {
    super(VersionType.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_VERSION_TYPE;
  }

  @Override
  protected Map<VersionType, String> expectedDimensionValueMapping() {
    return CollectionUtils.<VersionType, String>mapBuilder()
        .put(VersionType.CURRENT, "current")
        .put(VersionType.FUTURE, "future")
        .put(VersionType.OTHER, "other")
        .build();
  }
}
