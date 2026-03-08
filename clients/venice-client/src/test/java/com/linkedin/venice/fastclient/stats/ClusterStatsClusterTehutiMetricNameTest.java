package com.linkedin.venice.fastclient.stats;

import com.linkedin.venice.fastclient.stats.ClusterStats.ClusterTehutiMetricName;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class ClusterStatsClusterTehutiMetricNameTest {
  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(ClusterTehutiMetricName.class, expectedMetricNames()).assertAll();
  }

  private static Map<ClusterTehutiMetricName, String> expectedMetricNames() {
    Map<ClusterTehutiMetricName, String> map = new HashMap<>();
    map.put(ClusterTehutiMetricName.VERSION_UPDATE_FAILURE, "version_update_failure");
    map.put(ClusterTehutiMetricName.CURRENT_VERSION, "current_version");
    map.put(ClusterTehutiMetricName.BLOCKED_INSTANCE_COUNT, "blocked_instance_count");
    map.put(ClusterTehutiMetricName.UNHEALTHY_INSTANCE_COUNT, "unhealthy_instance_count");
    map.put(ClusterTehutiMetricName.OVERLOADED_INSTANCE_COUNT, "overloaded_instance_count");
    return map;
  }
}
