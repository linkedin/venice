package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class VeniceIngestionDestinationComponentTest
    extends VeniceDimensionInterfaceTest<VeniceIngestionDestinationComponent> {
  protected VeniceIngestionDestinationComponentTest() {
    super(VeniceIngestionDestinationComponent.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_INGESTION_DESTINATION_COMPONENT;
  }

  @Override
  protected Map<VeniceIngestionDestinationComponent, String> expectedDimensionValueMapping() {
    return CollectionUtils.<VeniceIngestionDestinationComponent, String>mapBuilder()
        .put(VeniceIngestionDestinationComponent.LOCAL_BROKER, "local_broker")
        .put(VeniceIngestionDestinationComponent.SOURCE_BROKER, "source_broker")
        .put(VeniceIngestionDestinationComponent.LEADER_CONSUMER, "leader_consumer")
        .put(VeniceIngestionDestinationComponent.FOLLOWER_CONSUMER, "follower_consumer")
        .build();
  }
}
