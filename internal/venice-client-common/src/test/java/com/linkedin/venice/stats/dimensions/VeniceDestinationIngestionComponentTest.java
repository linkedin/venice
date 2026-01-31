package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class VeniceDestinationIngestionComponentTest
    extends VeniceDimensionInterfaceTest<VeniceDestinationIngestionComponent> {
  protected VeniceDestinationIngestionComponentTest() {
    super(VeniceDestinationIngestionComponent.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_INGESTION_DESTINATION_COMPONENT;
  }

  @Override
  protected Map<VeniceDestinationIngestionComponent, String> expectedDimensionValueMapping() {
    return CollectionUtils.<VeniceDestinationIngestionComponent, String>mapBuilder()
        .put(VeniceDestinationIngestionComponent.LOCAL_BROKER, "local_broker")
        .put(VeniceDestinationIngestionComponent.SOURCE_BROKER, "source_broker")
        .put(VeniceDestinationIngestionComponent.LEADER_CONSUMER, "leader_consumer")
        .put(VeniceDestinationIngestionComponent.FOLLOWER_CONSUMER, "follower_consumer")
        .build();
  }
}
