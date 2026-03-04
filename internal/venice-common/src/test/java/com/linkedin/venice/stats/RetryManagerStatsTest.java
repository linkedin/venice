package com.linkedin.venice.stats;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class RetryManagerStatsTest {
  @Test
  public void testRetryManagerTehutiMetricNameEnum() {
    Map<RetryManagerStats.RetryManagerTehutiMetricName, String> expectedNames = new HashMap<>();
    expectedNames
        .put(RetryManagerStats.RetryManagerTehutiMetricName.RETRY_LIMIT_PER_SECONDS, "retry_limit_per_seconds");
    expectedNames.put(RetryManagerStats.RetryManagerTehutiMetricName.RETRIES_REMAINING, "retries_remaining");
    expectedNames.put(RetryManagerStats.RetryManagerTehutiMetricName.REJECTED_RETRY, "rejected_retry");

    assertEquals(
        RetryManagerStats.RetryManagerTehutiMetricName.values().length,
        expectedNames.size(),
        "New RetryManagerTehutiMetricName values were added but not included in this test");

    for (RetryManagerStats.RetryManagerTehutiMetricName enumValue: RetryManagerStats.RetryManagerTehutiMetricName
        .values()) {
      String expectedName = expectedNames.get(enumValue);
      assertNotNull(expectedName, "No expected metric name for " + enumValue.name());
      assertEquals(enumValue.getMetricName(), expectedName, "Unexpected metric name for " + enumValue.name());
    }
  }
}
