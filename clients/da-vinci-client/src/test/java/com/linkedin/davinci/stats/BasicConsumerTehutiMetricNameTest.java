package com.linkedin.davinci.stats;

import com.linkedin.davinci.consumer.stats.BasicConsumerStats;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class BasicConsumerTehutiMetricNameTest {
  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(BasicConsumerStats.BasicConsumerTehutiMetricName.class, expectedMetricNames())
        .assertAll();
  }

  private static Map<BasicConsumerStats.BasicConsumerTehutiMetricName, String> expectedMetricNames() {
    Map<BasicConsumerStats.BasicConsumerTehutiMetricName, String> map = new HashMap<>();
    map.put(BasicConsumerStats.BasicConsumerTehutiMetricName.MAX_PARTITION_LAG, "max_partition_lag");
    map.put(BasicConsumerStats.BasicConsumerTehutiMetricName.RECORDS_CONSUMED, "records_consumed");
    map.put(BasicConsumerStats.BasicConsumerTehutiMetricName.MINIMUM_CONSUMING_VERSION, "minimum_consuming_version");
    map.put(BasicConsumerStats.BasicConsumerTehutiMetricName.MAXIMUM_CONSUMING_VERSION, "maximum_consuming_version");
    map.put(BasicConsumerStats.BasicConsumerTehutiMetricName.POLL_SUCCESS_COUNT, "poll_success_count");
    map.put(BasicConsumerStats.BasicConsumerTehutiMetricName.POLL_FAIL_COUNT, "poll_fail_count");
    map.put(BasicConsumerStats.BasicConsumerTehutiMetricName.VERSION_SWAP_SUCCESS_COUNT, "version_swap_success_count");
    map.put(BasicConsumerStats.BasicConsumerTehutiMetricName.VERSION_SWAP_FAIL_COUNT, "version_swap_fail_count");
    map.put(
        BasicConsumerStats.BasicConsumerTehutiMetricName.CHUNKED_RECORD_SUCCESS_COUNT,
        "chunked_record_success_count");
    map.put(BasicConsumerStats.BasicConsumerTehutiMetricName.CHUNKED_RECORD_FAIL_COUNT, "chunked_record_fail_count");
    return map;
  }
}
