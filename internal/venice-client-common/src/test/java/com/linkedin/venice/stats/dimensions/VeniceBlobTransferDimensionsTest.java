package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceBlobTransferDimensionsTest {
  @Test
  public void testSourceDimensionInterface() {
    Map<VeniceBlobTransferSource, String> expectedValues =
        CollectionUtils.<VeniceBlobTransferSource, String>mapBuilder()
            .put(VeniceBlobTransferSource.DAVINCI_PEER, "davinci_peer")
            .put(VeniceBlobTransferSource.VENICE_SERVER, "venice_server")
            .build();
    new VeniceDimensionTestFixture<>(
        VeniceBlobTransferSource.class,
        VeniceMetricsDimensions.VENICE_BLOB_TRANSFER_SOURCE,
        expectedValues).assertAll();
  }

  @Test
  public void testFallbackReasonDimensionInterface() {
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
