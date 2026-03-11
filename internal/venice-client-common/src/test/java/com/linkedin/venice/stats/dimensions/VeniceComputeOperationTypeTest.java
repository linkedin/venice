package com.linkedin.venice.stats.dimensions;

import static org.testng.Assert.assertEquals;

import com.linkedin.venice.compute.protocol.request.enums.ComputeOperationType;
import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceComputeOperationTypeTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceComputeOperationType, String> expectedValues =
        CollectionUtils.<VeniceComputeOperationType, String>mapBuilder()
            .put(VeniceComputeOperationType.DOT_PRODUCT, "dot_product")
            .put(VeniceComputeOperationType.COSINE_SIMILARITY, "cosine_similarity")
            .put(VeniceComputeOperationType.HADAMARD_PRODUCT, "hadamard_product")
            .put(VeniceComputeOperationType.COUNT, "count")
            .build();
    new VeniceDimensionTestFixture<>(
        VeniceComputeOperationType.class,
        VeniceMetricsDimensions.VENICE_READ_COMPUTE_OPERATION_TYPE,
        expectedValues).assertAll();
  }

  @Test
  public void testSyncWithProtocolComputeOperationType() {
    assertEquals(
        VeniceComputeOperationType.values().length,
        ComputeOperationType.values().length,
        "VeniceComputeOperationType must stay in sync with ComputeOperationType");
    for (ComputeOperationType protocolOp: ComputeOperationType.values()) {
      VeniceComputeOperationType.valueOf(protocolOp.name());
    }
  }
}
