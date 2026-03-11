package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceIngestionDestinationComponentTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceIngestionDestinationComponent, String> expectedValues =
        CollectionUtils.<VeniceIngestionDestinationComponent, String>mapBuilder()
            .put(VeniceIngestionDestinationComponent.LOCAL_BROKER, "local_broker")
            .put(VeniceIngestionDestinationComponent.SOURCE_BROKER, "source_broker")
            .put(VeniceIngestionDestinationComponent.LEADER_CONSUMER, "leader_consumer")
            .put(VeniceIngestionDestinationComponent.FOLLOWER_CONSUMER, "follower_consumer")
            .build();
    new VeniceDimensionTestFixture<>(
        VeniceIngestionDestinationComponent.class,
        VeniceMetricsDimensions.VENICE_INGESTION_DESTINATION_COMPONENT,
        expectedValues).assertAll();
  }
}
