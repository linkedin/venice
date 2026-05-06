package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.StuckConsumerRepairOtelMetricEntity.STUCK_CONSUMER_DETECTED_COUNT;
import static com.linkedin.davinci.stats.StuckConsumerRepairOtelMetricEntity.STUCK_CONSUMER_TASK_REPAIRED_COUNT;
import static com.linkedin.davinci.stats.StuckConsumerRepairOtelMetricEntity.STUCK_CONSUMER_UNRESOLVED_COUNT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class StuckConsumerRepairOtelMetricEntityTest {
  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(StuckConsumerRepairOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }

  private static Map<StuckConsumerRepairOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<StuckConsumerRepairOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        STUCK_CONSUMER_DETECTED_COUNT,
        new MetricEntityExpectation(
            "ingestion.pubsub.consumer.stuck.detected_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of scans that detected a stuck PubSub consumer",
            setOf(VENICE_CLUSTER_NAME)));
    map.put(
        STUCK_CONSUMER_TASK_REPAIRED_COUNT,
        new MetricEntityExpectation(
            "ingestion.pubsub.consumer.stuck.task_repaired_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of ingestion tasks killed to unblock a stuck PubSub consumer",
            setOf(VENICE_CLUSTER_NAME)));
    map.put(
        STUCK_CONSUMER_UNRESOLVED_COUNT,
        new MetricEntityExpectation(
            "ingestion.pubsub.consumer.stuck.unresolved_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of scans where a stuck PubSub consumer was found but no fixable task identified",
            setOf(VENICE_CLUSTER_NAME)));
    return map;
  }
}
