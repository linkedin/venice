package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.pubsub.PubSubHealthCategory;
import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class PubSubHealthCategoryTest extends VeniceDimensionInterfaceTest<PubSubHealthCategory> {
  protected PubSubHealthCategoryTest() {
    super(PubSubHealthCategory.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_PUBSUB_HEALTH_CATEGORY;
  }

  @Override
  protected Map<PubSubHealthCategory, String> expectedDimensionValueMapping() {
    return CollectionUtils.<PubSubHealthCategory, String>mapBuilder()
        .put(PubSubHealthCategory.BROKER, "broker")
        .put(PubSubHealthCategory.METADATA_SERVICE, "metadata_service")
        .build();
  }
}
