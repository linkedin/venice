package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class VersionRoleTest extends VeniceDimensionInterfaceTest<VersionRole> {
  protected VersionRoleTest() {
    super(VersionRole.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_VERSION_ROLE;
  }

  @Override
  protected Map<VersionRole, String> expectedDimensionValueMapping() {
    return CollectionUtils.<VersionRole, String>mapBuilder()
        .put(VersionRole.CURRENT, "current")
        .put(VersionRole.FUTURE, "future")
        .put(VersionRole.BACKUP, "backup")
        .build();
  }
}
