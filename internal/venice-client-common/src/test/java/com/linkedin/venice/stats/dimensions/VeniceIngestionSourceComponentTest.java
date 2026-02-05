package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class VeniceIngestionSourceComponentTest extends VeniceDimensionInterfaceTest<VeniceIngestionSourceComponent> {
  protected VeniceIngestionSourceComponentTest() {
    super(VeniceIngestionSourceComponent.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_INGESTION_SOURCE_COMPONENT;
  }

  @Override
  protected Map<VeniceIngestionSourceComponent, String> expectedDimensionValueMapping() {
    return CollectionUtils.<VeniceIngestionSourceComponent, String>mapBuilder()
        .put(VeniceIngestionSourceComponent.PRODUCER, "producer")
        .put(VeniceIngestionSourceComponent.LOCAL_BROKER, "local_broker")
        .put(VeniceIngestionSourceComponent.SOURCE_BROKER, "source_broker")
        .put(VeniceIngestionSourceComponent.LEADER_CONSUMER, "leader_consumer")
        .put(VeniceIngestionSourceComponent.FOLLOWER_CONSUMER, "follower_consumer")
        .build();
  }
}
