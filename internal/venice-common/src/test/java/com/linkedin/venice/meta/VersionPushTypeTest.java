package com.linkedin.venice.meta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


/**
 * Tests that {@link Version.PushType} correctly implements
 * {@link com.linkedin.venice.stats.dimensions.VeniceDimensionInterface}.
 * Mirrors the pattern in {@code VeniceDimensionInterfaceTest} but is self-contained
 * because the abstract base class lives in {@code venice-client-common} test sources,
 * which are not available as a test dependency for {@code venice-common}.
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

    assertEquals(
        Version.PushType.values().length,
        expectedValues.size(),
        "New PushType values were added but not included in this test");

    for (Version.PushType pushType: Version.PushType.values()) {
      String expectedValue = expectedValues.get(pushType);
      assertNotNull(expectedValue, "No expected dimension value for " + pushType.name());

      assertEquals(
          pushType.getDimensionName(),
          VeniceMetricsDimensions.VENICE_PUSH_JOB_TYPE,
          "Unexpected dimension name for " + pushType.name());
      assertEquals(pushType.getDimensionValue(), expectedValue, "Unexpected dimension value for " + pushType.name());
    }
  }
}
