package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;


public class TopicCleanupServiceStats extends AbstractVeniceStats {
  private final Sensor deletableTopicsCountGaugeSensor;
  private final Sensor deletedTopicsCountSensor;
  private final Sensor topicDeletionErrorCountSensor;

  public TopicCleanupServiceStats(MetricsRepository metricsRepository) {
    super(metricsRepository, "TopicCleanupService");
    deletableTopicsCountGaugeSensor = registerSensorIfAbsent("deletable_topics_count", new Gauge());
    deletedTopicsCountSensor = registerSensorIfAbsent("deleted_topics_count", new Count());
    topicDeletionErrorCountSensor = registerSensorIfAbsent("topic_deletion_error_count", new Count());
  }

  public void recordDeletableTopicsCount(int deletableTopicsCount) {
    deletableTopicsCountGaugeSensor.record(deletableTopicsCount);
  }

  public void recordDeletedTopicsCount() {
    deletedTopicsCountSensor.record();
  }

  public void recordTopicDeletionError() {
    topicDeletionErrorCountSensor.record();
  }
}
