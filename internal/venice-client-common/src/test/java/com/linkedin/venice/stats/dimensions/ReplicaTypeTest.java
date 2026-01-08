package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class ReplicaTypeTest extends VeniceDimensionInterfaceTest<ReplicaType> {
  protected ReplicaTypeTest() {
    super(ReplicaType.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_REPLICA_TYPE;
  }

  @Override
  protected Map<ReplicaType, String> expectedDimensionValueMapping() {
    return CollectionUtils.<ReplicaType, String>mapBuilder()
        .put(ReplicaType.LEADER, "leader")
        .put(ReplicaType.FOLLOWER, "follower")
        .build();
  }
}
