package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class VeniceChunkingStatusTest extends VeniceDimensionInterfaceTest<VeniceChunkingStatus> {
  protected VeniceChunkingStatusTest() {
    super(VeniceChunkingStatus.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_CHUNKING_STATUS;
  }

  @Override
  protected Map<VeniceChunkingStatus, String> expectedDimensionValueMapping() {
    return CollectionUtils.<VeniceChunkingStatus, String>mapBuilder()
        .put(VeniceChunkingStatus.CHUNKED, "chunked")
        .put(VeniceChunkingStatus.UNCHUNKED, "unchunked")
        .build();
  }
}
