package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class ReplicaStateTest {
  @Test
  public void testDimensionInterface() {
    Map<ReplicaState, String> expectedValues = CollectionUtils.<ReplicaState, String>mapBuilder()
        .put(ReplicaState.READY_TO_SERVE, "ready_to_serve")
        .put(ReplicaState.CATCHING_UP, "catching_up")
        .build();
    new VeniceDimensionTestFixture<>(ReplicaState.class, VeniceMetricsDimensions.VENICE_REPLICA_STATE, expectedValues)
        .assertAll();
  }
}
