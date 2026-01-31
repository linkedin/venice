package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class VeniceIngestionComponentTest extends VeniceDimensionInterfaceTest<VeniceIngestionComponent> {
  protected VeniceIngestionComponentTest() {
    super(VeniceIngestionComponent.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_INGESTION_SOURCE_COMPONENT;
  }

  @Override
  protected Map<VeniceIngestionComponent, String> expectedDimensionValueMapping() {
    return CollectionUtils.<VeniceIngestionComponent, String>mapBuilder()
        .put(VeniceIngestionComponent.PRODUCER, "producer")
        .put(VeniceIngestionComponent.LOCAL_BROKER, "local_broker")
        .put(VeniceIngestionComponent.SOURCE_BROKER, "source_broker")
        .put(VeniceIngestionComponent.LEADER_CONSUMER, "leader_consumer")
        .put(VeniceIngestionComponent.FOLLOWER_CONSUMER, "follower_consumer")
        .build();
  }
}
