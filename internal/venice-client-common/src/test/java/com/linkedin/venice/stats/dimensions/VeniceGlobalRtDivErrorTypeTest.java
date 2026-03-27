package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceGlobalRtDivErrorTypeTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceGlobalRtDivErrorType, String> expectedValues =
        CollectionUtils.<VeniceGlobalRtDivErrorType, String>mapBuilder()
            .put(VeniceGlobalRtDivErrorType.SEND, "send")
            .put(VeniceGlobalRtDivErrorType.PERSIST, "persist")
            .put(VeniceGlobalRtDivErrorType.VT_SYNC, "vt_sync")
            .put(VeniceGlobalRtDivErrorType.DELETE, "delete")
            .build();
    new VeniceDimensionTestFixture<>(
        VeniceGlobalRtDivErrorType.class,
        VeniceMetricsDimensions.VENICE_GLOBAL_RT_DIV_ERROR_TYPE,
        expectedValues).assertAll();
  }
}
