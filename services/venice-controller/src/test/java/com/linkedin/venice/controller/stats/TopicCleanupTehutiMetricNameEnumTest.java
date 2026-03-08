package com.linkedin.venice.controller.stats;

import com.linkedin.venice.controller.stats.TopicCleanupServiceStats.TopicCleanupTehutiMetricNameEnum;
import com.linkedin.venice.stats.metrics.AbstractTehutiMetricNameEnumTest;
import java.util.HashMap;
import java.util.Map;


public class TopicCleanupTehutiMetricNameEnumTest
    extends AbstractTehutiMetricNameEnumTest<TopicCleanupTehutiMetricNameEnum> {
  public TopicCleanupTehutiMetricNameEnumTest() {
    super(TopicCleanupTehutiMetricNameEnum.class);
  }

  @Override
  protected Map<TopicCleanupTehutiMetricNameEnum, String> expectedMetricNames() {
    Map<TopicCleanupTehutiMetricNameEnum, String> map = new HashMap<>();
    map.put(TopicCleanupTehutiMetricNameEnum.DELETABLE_TOPICS_COUNT, "deletable_topics_count");
    map.put(TopicCleanupTehutiMetricNameEnum.TOPICS_DELETED_RATE, "topics_deleted_rate");
    map.put(TopicCleanupTehutiMetricNameEnum.TOPIC_DELETION_ERROR_RATE, "topic_deletion_error_rate");
    return map;
  }
}
