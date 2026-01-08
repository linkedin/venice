package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class ReplicaStateTest extends VeniceDimensionInterfaceTest<ReplicaState> {
  protected ReplicaStateTest() {
    super(ReplicaState.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_REPLICA_STATE;
  }

  @Override
  protected Map<ReplicaState, String> expectedDimensionValueMapping() {
    return CollectionUtils.<ReplicaState, String>mapBuilder()
        .put(ReplicaState.READY_TO_SERVE, "ready_to_serve")
        .put(ReplicaState.CATCHING_UP, "catching_up")
        .build();
  }
}
