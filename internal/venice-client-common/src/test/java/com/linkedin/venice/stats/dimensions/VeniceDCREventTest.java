package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class VeniceDCREventTest extends VeniceDimensionInterfaceTest<VeniceDCREvent> {
  protected VeniceDCREventTest() {
    super(VeniceDCREvent.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_DCR_EVENT;
  }

  @Override
  protected Map<VeniceDCREvent, String> expectedDimensionValueMapping() {
    return CollectionUtils.<VeniceDCREvent, String>mapBuilder()
        .put(VeniceDCREvent.UPDATE_IGNORED, "update_ignored")
        .put(VeniceDCREvent.TOMBSTONE_CREATION, "tombstone_creation")
        .put(VeniceDCREvent.TIMESTAMP_REGRESSION_ERROR, "timestamp_regression_error")
        .put(VeniceDCREvent.OFFSET_REGRESSION_ERROR, "offset_regression_error")
        .build();
  }
}
