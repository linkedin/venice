package com.linkedin.venice.stats.dimensions;

import java.util.LinkedHashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceBlobTransferOutcomeTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceBlobTransferOutcome, String> expectedValues = new LinkedHashMap<>();
    for (VeniceBlobTransferOutcome outcome: VeniceBlobTransferOutcome.values()) {
      expectedValues.put(outcome, outcome.name().toLowerCase());
    }
    new VeniceDimensionTestFixture<>(
        VeniceBlobTransferOutcome.class,
        VeniceMetricsDimensions.VENICE_BLOB_TRANSFER_OUTCOME,
        expectedValues).assertAll();
  }
}
