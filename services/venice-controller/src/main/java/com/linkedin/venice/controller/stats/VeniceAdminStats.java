package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.LatencyUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;


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

  /**
   * A counter reporting the number of failed serialization attempts of admin operations.
   * This metric is used to monitor the health of serialization of admin operations in parent admin by using the dynamic
   * version in the serializer.
   */
  private final Sensor failedSerializingAdminOperationMessageCount;

  /** Measure latency when sending the start-of-push signal
   * for {@link com.linkedin.venice.controller.VeniceHelixAdmin#writeEndOfPush(String, String, int, boolean)}.*/
  private final Sensor startOfPushLatencySensor;

  /** Measure latency when sending the end-of-push signal to the queue
   * for {@link com.linkedin.venice.controller.VeniceHelixAdmin#writeEndOfPush(String, String, int, boolean)}.*/
  private final Sensor endOfPushLatencySensor;

  /** Tracks latency for producer flush operation
   * for {@link com.linkedin.venice.controller.VeniceHelixAdmin#writeEndOfPush(String, String, int, boolean)}.*/
  private final Sensor producerFlushLatencySensor;

  public VeniceAdminStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    unexpectedTopicAbsenceDuringIncrementalPushCountSensor =
        registerSensorIfAbsent("unexpected_topic_absence_during_incremental_push_count", new Count());
    successfullyStartedUserBatchPushParentAdminCountSensor =
        registerSensorIfAbsent("successfully_started_user_batch_push_parent_admin_count", new Count());
    successfullyStartedUserIncrementalPushParentAdminCountSensor =
        registerSensorIfAbsent("successful_started_user_incremental_push_parent_admin_count", new Count());
    failedSerializingAdminOperationMessageCount =
        registerSensorIfAbsent("failed_serializing_admin_operation_message_count", new Count());

    String startOfPushLatencySensorName = "start_of_push_latency";
    startOfPushLatencySensor = registerSensorIfAbsent(
        startOfPushLatencySensorName,
        new Avg(),
        new Max(),
        TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + startOfPushLatencySensorName));
    String endOfPushLatencySensorName = "end_of_push_latency";
    endOfPushLatencySensor = registerSensorIfAbsent(
        endOfPushLatencySensorName,
        new Avg(),
        new Max(),
        TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + endOfPushLatencySensorName));

    String producerFlushLatencySensorName = "producer_flush_latency";
    producerFlushLatencySensor = registerSensorIfAbsent(
        producerFlushLatencySensorName,
        new Avg(),
        new Max(),
        TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + producerFlushLatencySensorName));

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

  public void recordFailedSerializingAdminOperationMessageCount() {
    failedSerializingAdminOperationMessageCount.record();
  }

  public void recordStartOfPushLatency(long startTime) {
    startOfPushLatencySensor.record(LatencyUtils.getElapsedTimeFromMsToMs(startTime));
  }

  public void recordEndOfPushLatency(long startTime) {
    endOfPushLatencySensor.record(LatencyUtils.getElapsedTimeFromMsToMs(startTime));
  }

  public void recordProducerFlushLatency(long startTime) {
    producerFlushLatencySensor.record(LatencyUtils.getElapsedTimeFromMsToMs(startTime));
  }

}
