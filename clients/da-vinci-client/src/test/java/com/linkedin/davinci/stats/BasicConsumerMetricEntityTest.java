package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.davinci.consumer.stats.BasicConsumerStats;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class BasicConsumerMetricEntityTest {
  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(BasicConsumerStats.BasicConsumerMetricEntity.class, expectedDefinitions())
        .assertAll();
  }

  private static Map<BasicConsumerStats.BasicConsumerMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<BasicConsumerStats.BasicConsumerMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        BasicConsumerStats.BasicConsumerMetricEntity.HEART_BEAT_DELAY,
        new MetricEntityExpectation(
            "heart_beat_delay",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Measures the max heartbeat delay across all subscribed partitions",
            setOf(VENICE_STORE_NAME)));
    map.put(
        BasicConsumerStats.BasicConsumerMetricEntity.CURRENT_CONSUMING_VERSION,
        new MetricEntityExpectation(
            "current_consuming_version",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.NUMBER,
            "Measures the min/max consuming version across all subscribed partitions",
            setOf(VENICE_STORE_NAME)));
    map.put(
        BasicConsumerStats.BasicConsumerMetricEntity.RECORDS_CONSUMED_COUNT,
        new MetricEntityExpectation(
            "records_consumed_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Measures the count of records consumed",
            setOf(VENICE_STORE_NAME)));
    map.put(
        BasicConsumerStats.BasicConsumerMetricEntity.POLL_COUNT,
        new MetricEntityExpectation(
            "poll_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Measures the count of poll invocations",
            setOf(VENICE_STORE_NAME, VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    map.put(
        BasicConsumerStats.BasicConsumerMetricEntity.VERSION_SWAP_COUNT,
        new MetricEntityExpectation(
            "version_swap_count",
            MetricType.UP_DOWN_COUNTER,
            MetricUnit.NUMBER,
            "Measures the count of version swaps",
            setOf(VENICE_STORE_NAME, VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    map.put(
        BasicConsumerStats.BasicConsumerMetricEntity.CHUNKED_RECORD_COUNT,
        new MetricEntityExpectation(
            "chunked_record_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Measures the count of chunked records consumed",
            setOf(VENICE_STORE_NAME, VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    return map;
  }
}
