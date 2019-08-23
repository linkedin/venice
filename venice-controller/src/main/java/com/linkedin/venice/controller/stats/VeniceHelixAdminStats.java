package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;


public class VeniceHelixAdminStats extends AbstractVeniceStats {
  /**
   * A counter reporting errors due to absence of a kafka topic that is expected to be available. e.g. the version topic
   * is absent during an incremental push.
   */
  final private Sensor unexpectedTopicAbsenceCountSensor;

  public VeniceHelixAdminStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    unexpectedTopicAbsenceCountSensor = registerSensor("unexpected_topic_absence_count", new Count());
  }

  public void recordUnexpectedTopicAbsenceCount() {
    unexpectedTopicAbsenceCountSensor.record();
  }
}
