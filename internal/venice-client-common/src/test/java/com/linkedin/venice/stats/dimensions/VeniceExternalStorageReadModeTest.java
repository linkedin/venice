package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceExternalStorageReadModeTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceExternalStorageReadMode, String> expectedValues =
        CollectionUtils.<VeniceExternalStorageReadMode, String>mapBuilder()
            .put(VeniceExternalStorageReadMode.VENICE_ONLY, "venice_only")
            .put(VeniceExternalStorageReadMode.EXTERNAL_ONLY, "external_only")
            .put(VeniceExternalStorageReadMode.DUAL_MODE_CONSISTENCY_CHECK, "dual_mode_consistency_check")
            .put(VeniceExternalStorageReadMode.DUAL_MODE_EARLY_RETURN, "dual_mode_early_return")
            .build();
    new VeniceDimensionTestFixture<>(
        VeniceExternalStorageReadMode.class,
        VeniceMetricsDimensions.VENICE_EXTERNAL_STORAGE_READ_MODE,
        expectedValues).assertAll();
  }
}
