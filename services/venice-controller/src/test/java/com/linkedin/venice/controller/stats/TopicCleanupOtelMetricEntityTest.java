package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.controller.stats.TopicCleanupServiceStats.TopicCleanupOtelMetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class TopicCleanupOtelMetricEntityTest {
  private static Map<TopicCleanupOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<TopicCleanupOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        TopicCleanupOtelMetricEntity.TOPIC_CLEANUP_DELETABLE_COUNT,
        new MetricEntityExpectation(
            "topic_cleanup_service.topic.deletable_count",
            MetricType.GAUGE,
            MetricUnit.NUMBER,
            "Count of topics currently eligible for deletion",
            Collections.emptySet()));
    map.put(
        TopicCleanupOtelMetricEntity.TOPIC_CLEANUP_DELETED_COUNT,
        new MetricEntityExpectation(
            "topic_cleanup_service.topic.deleted_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of topic deletion operations",
            setOf(VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    return map;
  }

  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(TopicCleanupOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }
}
