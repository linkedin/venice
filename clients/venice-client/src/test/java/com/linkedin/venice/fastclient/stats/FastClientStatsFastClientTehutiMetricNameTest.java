package com.linkedin.venice.fastclient.stats;

import com.linkedin.venice.fastclient.stats.FastClientStats.FastClientTehutiMetricName;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class FastClientStatsFastClientTehutiMetricNameTest {
  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(FastClientTehutiMetricName.class, expectedMetricNames()).assertAll();
  }

  private static Map<FastClientTehutiMetricName, String> expectedMetricNames() {
    Map<FastClientTehutiMetricName, String> map = new HashMap<>();
    map.put(FastClientTehutiMetricName.LONG_TAIL_RETRY_REQUEST, "long_tail_retry_request");
    map.put(FastClientTehutiMetricName.ERROR_RETRY_REQUEST, "error_retry_request");
    map.put(FastClientTehutiMetricName.RETRY_REQUEST_WIN, "retry_request_win");
    map.put(FastClientTehutiMetricName.METADATA_STALENESS_HIGH_WATERMARK_MS, "metadata_staleness_high_watermark_ms");
    map.put(FastClientTehutiMetricName.FANOUT_SIZE, "fanout_size");
    map.put(FastClientTehutiMetricName.RETRY_FANOUT_SIZE, "retry_fanout_size");
    map.put(FastClientTehutiMetricName.NO_AVAILABLE_REPLICA_REQUEST_COUNT, "no_available_replica_request_count");
    map.put(
        FastClientTehutiMetricName.REJECTED_REQUEST_COUNT_BY_LOAD_CONTROLLER,
        "rejected_request_count_by_load_controller");
    map.put(FastClientTehutiMetricName.REJECTION_RATIO, "rejection_ratio");
    return map;
  }
}
