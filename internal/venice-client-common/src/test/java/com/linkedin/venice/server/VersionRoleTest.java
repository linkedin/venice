package com.linkedin.venice.server;

import com.linkedin.venice.stats.dimensions.VeniceDimensionTestFixture;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VersionRoleTest {
  @Test
  public void testDimensionInterface() {
    Map<VersionRole, String> expectedValues = CollectionUtils.<VersionRole, String>mapBuilder()
        .put(VersionRole.CURRENT, "current")
        .put(VersionRole.FUTURE, "future")
        .put(VersionRole.BACKUP, "backup")
        .build();
    new VeniceDimensionTestFixture<>(VersionRole.class, VeniceMetricsDimensions.VENICE_VERSION_ROLE, expectedValues)
        .assertAll();
  }
}
