package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;


public class VeniceAdminStats extends AbstractVeniceStats {
  /**
   * A counter reporting errors due to absence of a kafka topic that is expected to be available. e.g. the version topic
   * is absent during an incremental push.
   */
  private final Sensor unexpectedTopicAbsenceDuringIncrementalPushCountSensor;

  /**
   * A counter reporting successfully started user batch pushes from the Venice parent admin's perspective.
   * i.e. this metric doesn't include version creation triggered by store migration or system stores. This metric is
   * used as another data point to validate/monitor push job details reporting.
   */
  private final Sensor successfullyStartedUserBatchPushParentAdminCountSensor;

  /**
   * A counter reporting successfully started user incremental pushes from the Venice parent admin's perspective.
   * This metric is used as another data point to validate/monitor push job details reporting.
   */
  private final Sensor successfullyStartedUserIncrementalPushParentAdminCountSensor;

  public VeniceAdminStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    unexpectedTopicAbsenceDuringIncrementalPushCountSensor =
        registerSensorIfAbsent("unexpected_topic_absence_during_incremental_push_count", new Count());
    successfullyStartedUserBatchPushParentAdminCountSensor =
        registerSensorIfAbsent("successfully_started_user_batch_push_parent_admin_count", new Count());
    successfullyStartedUserIncrementalPushParentAdminCountSensor =
        registerSensorIfAbsent("successful_started_user_incremental_push_parent_admin_count", new Count());
  }

  public void recordUnexpectedTopicAbsenceCount() {
    unexpectedTopicAbsenceDuringIncrementalPushCountSensor.record();
  }

  public void recordSuccessfullyStartedUserBatchPushParentAdminCount() {
    successfullyStartedUserBatchPushParentAdminCountSensor.record();
  }

  public void recordSuccessfullyStartedUserIncrementalPushParentAdminCount() {
    successfullyStartedUserIncrementalPushParentAdminCountSensor.record();
  }
}
