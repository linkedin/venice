package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class StreamProgressTest {
  @Test
  public void testDimensionInterface() {
    Map<StreamProgress, String> expectedValues = CollectionUtils.<StreamProgress, String>mapBuilder()
        .put(StreamProgress.FIRST, "FirstRecord")
        .put(StreamProgress.PCT_50, "50thPercentileRecord")
        .put(StreamProgress.PCT_90, "90thPercentileRecord")
        .build();
    new VeniceDimensionTestFixture<>(
        StreamProgress.class,
        VeniceMetricsDimensions.VENICE_STREAM_PROGRESS,
        expectedValues).assertAll();
  }
}
