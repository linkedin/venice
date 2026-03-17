package com.linkedin.venice.stats;

import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class ServerReadQuotaTehutiMetricNameTest {
  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(ServerReadQuotaUsageStats.TehutiMetricName.class, expectedMetricNames())
        .assertAll();
  }

  private static Map<ServerReadQuotaUsageStats.TehutiMetricName, String> expectedMetricNames() {
    Map<ServerReadQuotaUsageStats.TehutiMetricName, String> map = new HashMap<>();
    map.put(ServerReadQuotaUsageStats.TehutiMetricName.CURRENT_QUOTA_REQUEST, "current_quota_request");
    map.put(ServerReadQuotaUsageStats.TehutiMetricName.BACKUP_QUOTA_REQUEST, "backup_quota_request");
    map.put(
        ServerReadQuotaUsageStats.TehutiMetricName.CURRENT_QUOTA_REQUEST_KEY_COUNT,
        "current_quota_request_key_count");
    map.put(
        ServerReadQuotaUsageStats.TehutiMetricName.BACKUP_QUOTA_REQUEST_KEY_COUNT,
        "backup_quota_request_key_count");
    map.put(ServerReadQuotaUsageStats.TehutiMetricName.QUOTA_REJECTED_REQUEST, "quota_rejected_request");
    map.put(ServerReadQuotaUsageStats.TehutiMetricName.QUOTA_REJECTED_KEY_COUNT, "quota_rejected_key_count");
    map.put(
        ServerReadQuotaUsageStats.TehutiMetricName.QUOTA_UNINTENTIONALLY_ALLOWED_KEY_COUNT,
        "quota_unintentionally_allowed_key_count");
    map.put(ServerReadQuotaUsageStats.TehutiMetricName.QUOTA_REQUESTED_USAGE_RATIO, "quota_requested_usage_ratio");
    return map;
  }
}
