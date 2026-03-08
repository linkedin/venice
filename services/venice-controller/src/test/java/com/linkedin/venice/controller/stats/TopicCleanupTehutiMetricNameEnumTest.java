package com.linkedin.venice.controller.stats;

import com.linkedin.venice.controller.stats.TopicCleanupServiceStats.TopicCleanupTehutiMetricNameEnum;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class TopicCleanupTehutiMetricNameEnumTest {
  private static Map<TopicCleanupTehutiMetricNameEnum, String> expectedMetricNames() {
    Map<TopicCleanupTehutiMetricNameEnum, String> map = new HashMap<>();
    map.put(TopicCleanupTehutiMetricNameEnum.DELETABLE_TOPICS_COUNT, "deletable_topics_count");
    map.put(TopicCleanupTehutiMetricNameEnum.TOPICS_DELETED_RATE, "topics_deleted_rate");
    map.put(TopicCleanupTehutiMetricNameEnum.TOPIC_DELETION_ERROR_RATE, "topic_deletion_error_rate");
    return map;
  }

  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(TopicCleanupTehutiMetricNameEnum.class, expectedMetricNames()).assertAll();
  }
}
