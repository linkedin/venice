package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceBlobTransferFallbackReasonTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceBlobTransferFallbackReason, String> expectedValues =
        CollectionUtils.<VeniceBlobTransferFallbackReason, String>mapBuilder()
            .put(VeniceBlobTransferFallbackReason.NO_CANDIDATES, "no_candidates")
            .put(VeniceBlobTransferFallbackReason.ALL_HOSTS_FAILED, "all_hosts_failed")
            .build();
    new VeniceDimensionTestFixture<>(
        VeniceBlobTransferFallbackReason.class,
        VeniceMetricsDimensions.VENICE_BLOB_TRANSFER_FALLBACK_REASON,
        expectedValues).assertAll();
  }
}
