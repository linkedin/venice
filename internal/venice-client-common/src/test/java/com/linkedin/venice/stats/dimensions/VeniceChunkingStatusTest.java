package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceChunkingStatusTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceChunkingStatus, String> expectedValues = CollectionUtils.<VeniceChunkingStatus, String>mapBuilder()
        .put(VeniceChunkingStatus.CHUNKED, "chunked")
        .put(VeniceChunkingStatus.UNCHUNKED, "unchunked")
        .build();
    new VeniceDimensionTestFixture<>(
        VeniceChunkingStatus.class,
        VeniceMetricsDimensions.VENICE_CHUNKING_STATUS,
        expectedValues).assertAll();
  }
}
