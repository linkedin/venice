package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Gauge;
import io.tehuti.metrics.stats.Rate;


public class TopicCleanupServiceStats extends AbstractVeniceStats {
  private final Sensor deletableTopicsCountGaugeSensor;
  private final Sensor topicsDeletedRateSensor;
  private final Sensor topicDeletionErrorRateSensor;

  public TopicCleanupServiceStats(MetricsRepository metricsRepository) {
    super(metricsRepository, "TopicCleanupService");
    deletableTopicsCountGaugeSensor = registerSensorIfAbsent("deletable_topics_count", new Gauge());
    topicsDeletedRateSensor = registerSensorIfAbsent("topics_deleted_rate", new Rate());
    topicDeletionErrorRateSensor = registerSensorIfAbsent("topic_deletion_error_rate", new Rate());
  }

  public void recordDeletableTopicsCount(int deletableTopicsCount) {
    deletableTopicsCountGaugeSensor.record(deletableTopicsCount);
  }

  public void recordTopicDeleted() {
    topicsDeletedRateSensor.record();
  }

  public void recordTopicDeletionError() {
    topicDeletionErrorRateSensor.record();
  }
}
