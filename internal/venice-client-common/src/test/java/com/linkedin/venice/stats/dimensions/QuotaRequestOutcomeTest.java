package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class QuotaRequestOutcomeTest {
  @Test
  public void testDimensionInterface() {
    Map<QuotaRequestOutcome, String> expectedValues = CollectionUtils.<QuotaRequestOutcome, String>mapBuilder()
        .put(QuotaRequestOutcome.ALLOWED, "allowed")
        .put(QuotaRequestOutcome.REJECTED, "rejected")
        .put(QuotaRequestOutcome.ALLOWED_UNINTENTIONALLY, "allowed_unintentionally")
        .build();
    new VeniceDimensionTestFixture<>(
        QuotaRequestOutcome.class,
        VeniceMetricsDimensions.VENICE_QUOTA_REQUEST_OUTCOME,
        expectedValues).assertAll();
  }
}
