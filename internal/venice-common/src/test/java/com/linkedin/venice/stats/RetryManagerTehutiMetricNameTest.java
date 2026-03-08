package com.linkedin.venice.stats;

import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class RetryManagerTehutiMetricNameTest {
  private static Map<RetryManagerStats.RetryManagerTehutiMetricName, String> expectedMetricNames() {
    Map<RetryManagerStats.RetryManagerTehutiMetricName, String> map = new HashMap<>();
    map.put(RetryManagerStats.RetryManagerTehutiMetricName.RETRY_LIMIT_PER_SECONDS, "retry_limit_per_seconds");
    map.put(RetryManagerStats.RetryManagerTehutiMetricName.RETRIES_REMAINING, "retries_remaining");
    map.put(RetryManagerStats.RetryManagerTehutiMetricName.REJECTED_RETRY, "rejected_retry");
    return map;
  }

  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(RetryManagerStats.RetryManagerTehutiMetricName.class, expectedMetricNames())
        .assertAll();
  }
}
