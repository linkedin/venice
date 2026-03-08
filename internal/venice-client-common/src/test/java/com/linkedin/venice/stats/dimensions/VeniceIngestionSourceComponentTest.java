package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceIngestionSourceComponentTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceIngestionSourceComponent, String> expectedValues =
        CollectionUtils.<VeniceIngestionSourceComponent, String>mapBuilder()
            .put(VeniceIngestionSourceComponent.PRODUCER, "producer")
            .put(VeniceIngestionSourceComponent.LOCAL_BROKER, "local_broker")
            .put(VeniceIngestionSourceComponent.SOURCE_BROKER, "source_broker")
            .build();
    new VeniceDimensionTestFixture<>(
        VeniceIngestionSourceComponent.class,
        VeniceMetricsDimensions.VENICE_INGESTION_SOURCE_COMPONENT,
        expectedValues).assertAll();
  }
}
