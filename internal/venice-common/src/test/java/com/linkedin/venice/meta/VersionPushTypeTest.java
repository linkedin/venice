package com.linkedin.venice.meta;

import com.linkedin.venice.stats.dimensions.VeniceDimensionTestFixture;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


/**
 * Tests that {@link Version.PushType} correctly implements
 * {@link com.linkedin.venice.stats.dimensions.VeniceDimensionInterface}.
 */
public class VersionPushTypeTest {
  @Test
  public void testDimensionInterface() {
    Map<Version.PushType, String> expectedValues = CollectionUtils.<Version.PushType, String>mapBuilder()
        .put(Version.PushType.BATCH, "batch")
        .put(Version.PushType.STREAM_REPROCESSING, "stream_reprocessing")
        .put(Version.PushType.STREAM, "stream")
        .put(Version.PushType.INCREMENTAL, "incremental")
        .build();
    new VeniceDimensionTestFixture<>(
        Version.PushType.class,
        VeniceMetricsDimensions.VENICE_PUSH_JOB_TYPE,
        expectedValues).assertAll();
  }
}
