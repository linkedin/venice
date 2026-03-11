package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class ReplicaTypeTest {
  @Test
  public void testDimensionInterface() {
    Map<ReplicaType, String> expectedValues = CollectionUtils.<ReplicaType, String>mapBuilder()
        .put(ReplicaType.LEADER, "leader")
        .put(ReplicaType.FOLLOWER, "follower")
        .build();
    new VeniceDimensionTestFixture<>(ReplicaType.class, VeniceMetricsDimensions.VENICE_REPLICA_TYPE, expectedValues)
        .assertAll();
  }
}
