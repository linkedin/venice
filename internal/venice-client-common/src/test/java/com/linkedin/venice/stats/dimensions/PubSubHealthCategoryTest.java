package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.pubsub.PubSubHealthCategory;
import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class PubSubHealthCategoryTest {
  @Test
  public void testDimensionInterface() {
    Map<PubSubHealthCategory, String> expectedValues = CollectionUtils.<PubSubHealthCategory, String>mapBuilder()
        .put(PubSubHealthCategory.BROKER, "broker")
        .put(PubSubHealthCategory.METADATA_SERVICE, "metadata_service")
        .build();
    new VeniceDimensionTestFixture<>(
        PubSubHealthCategory.class,
        VeniceMetricsDimensions.VENICE_PUBSUB_HEALTH_CATEGORY,
        expectedValues).assertAll();
  }
}
