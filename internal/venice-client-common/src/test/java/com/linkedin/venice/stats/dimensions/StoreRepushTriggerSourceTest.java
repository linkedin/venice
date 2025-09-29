package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class StoreRepushTriggerSourceTest extends VeniceDimensionInterfaceTest<StoreRepushTriggerSource> {
  protected StoreRepushTriggerSourceTest() {
    super(StoreRepushTriggerSource.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.STORE_REPUSH_TRIGGER_SOURCE;
  }

  @Override
  protected Map<StoreRepushTriggerSource, String> expectedDimensionValueMapping() {
    return CollectionUtils.<StoreRepushTriggerSource, String>mapBuilder()
        .put(StoreRepushTriggerSource.MANUAL, "manual")
        .put(StoreRepushTriggerSource.SCHEDULED_FOR_LOG_COMPACTION, "scheduled_for_log_compaction")
        .build();
  }
}
