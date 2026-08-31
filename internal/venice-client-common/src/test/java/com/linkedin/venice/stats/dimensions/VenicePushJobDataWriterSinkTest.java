package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VenicePushJobDataWriterSinkTest {
  @Test
  public void testDimensionInterface() {
    Map<VenicePushJobDataWriterSink, String> expectedValues =
        CollectionUtils.<VenicePushJobDataWriterSink, String>mapBuilder()
            .put(VenicePushJobDataWriterSink.VENICE, "venice")
            .put(VenicePushJobDataWriterSink.EXTERNAL_STORAGE, "external_storage")
            .build();
    new VeniceDimensionTestFixture<>(
        VenicePushJobDataWriterSink.class,
        VeniceMetricsDimensions.VENICE_PUSH_JOB_DATA_WRITER_SINK,
        expectedValues).assertAll();
  }
}
