package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class StreamProgressTest extends VeniceDimensionInterfaceTest<StreamProgress> {
  protected StreamProgressTest() {
    super(StreamProgress.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_STREAM_PROGRESS;
  }

  @Override
  protected Map<StreamProgress, String> expectedDimensionValueMapping() {
    return CollectionUtils.<StreamProgress, String>mapBuilder()
        .put(StreamProgress.FIRST, "FirstRecord")
        .put(StreamProgress.PCT_50, "50thPercentileRecord")
        .put(StreamProgress.PCT_90, "90thPercentileRecord")
        .build();
  }
}
