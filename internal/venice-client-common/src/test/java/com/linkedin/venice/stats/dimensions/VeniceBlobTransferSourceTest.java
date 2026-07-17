package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceBlobTransferSourceTest {
  @Test
  public void testDimensionInterface() {
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
}
