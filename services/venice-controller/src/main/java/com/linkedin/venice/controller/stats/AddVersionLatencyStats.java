package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.TehutiUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;


/**
 * This class is used to track the latency of various operations related to adding a version in Venice.
 * <p>
 * The following sensors are tracked:
 * <ul>
 *   <li><b>existingSourceVersionHandlingLatencySensor</b> –
 *       Measures latency when handling add version requests with an existing source version.</li>
 *   <li><b>batchTopicCreationLatencySensor</b> –
 *       Tracks latency for creating batch topics (used by both child and parent controllers).</li>
 *   <li><b>resourceAssignmentWaitLatencySensor</b> –
 *       Captures time spent waiting for node resource assignments.</li>
 *   <li><b>helixResourceCreationLatencySensor</b> –
 *       Monitors the latency of creating Helix storage cluster resources.</li>
 *   <li><b>retireOldVersionsLatencySensor</b> –
 *       Records the time taken to retire outdated store versions.</li>
 *   <li><b>startOfPushLatencySensor</b> –
 *       Measures latency for sending the start-of-push signal.</li>
 *   <li><b>versionCreationFailureLatencySensor</b> –
 *       Tracks latency during version creation failure handling.</li>
 * </ul>
 * Each metric uses milliseconds as the unit of measurement.
 */
public class AddVersionLatencyStats extends AbstractVeniceStats {
  /** Measures the time taken to retire outdated store versions. */
  private final Sensor retireOldVersionsLatencySensor;

  /** Captures the latency while waiting for node resource assignments. */
  private final Sensor resourceAssignmentWaitLatencySensor;

  /** Tracks latency during the handling of version creation failures. */
  private final Sensor versionCreationFailureLatencySensor;

  /** Measures latency for add version requests where the source version already exists. */
  private final Sensor existingSourceVersionHandlingLatencySensor;

  /** Records the time taken to send the start-of-push signal. */
  private final Sensor startOfPushLatencySensor;

  /** Tracks latency for creating batch topics, applicable to both parent and child controllers. */
  private final Sensor batchTopicCreationLatencySensor;

  /** Monitors the time required to create Helix storage cluster resources. */
  private final Sensor helixResourceCreationLatencySensor;

  public AddVersionLatencyStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    String retireOldVersionsLatencySensorName = "add_version_retire_old_versions_latency";
    retireOldVersionsLatencySensor = registerSensorIfAbsent(
        retireOldVersionsLatencySensorName,
        new Avg(),
        new Max(),
        TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + retireOldVersionsLatencySensorName));

    String resourceAssignmentWaitLatencySensorName = "add_version_resource_assignment_wait_latency";
    resourceAssignmentWaitLatencySensor = registerSensorIfAbsent(
        resourceAssignmentWaitLatencySensorName,
        new Avg(),
        new Max(),
        TehutiUtils
            .getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + resourceAssignmentWaitLatencySensorName));

    String versionCreationFailureLatencySensorName = "add_version_creation_failure_latency";
    versionCreationFailureLatencySensor = registerSensorIfAbsent(
        versionCreationFailureLatencySensorName,
        new Avg(),
        new Max(),
        TehutiUtils
            .getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + versionCreationFailureLatencySensorName));

    String existingSourceVersionHandlingLatencySensorName = "add_version_existing_source_handling_latency";
    existingSourceVersionHandlingLatencySensor = registerSensorIfAbsent(
        existingSourceVersionHandlingLatencySensorName,
        new Avg(),
        new Max(),
        TehutiUtils.getPercentileStat(
            getName() + AbstractVeniceStats.DELIMITER + existingSourceVersionHandlingLatencySensorName));

    String startOfPushLatencySensorName = "add_version_start_of_push_latency";
    startOfPushLatencySensor = registerSensorIfAbsent(
        startOfPushLatencySensorName,
        new Avg(),
        new Max(),
        TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + startOfPushLatencySensorName));

    String batchTopicCreationLatencySensorName = "add_version_batch_topic_creation_latency";
    batchTopicCreationLatencySensor = registerSensorIfAbsent(
        "add_version_batch_topic_creation_latency",
        new Avg(),
        new Max(),
        TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + batchTopicCreationLatencySensorName));

    String helixResourceCreationLatencySensorName = "add_version_helix_resource_creation_latency";
    helixResourceCreationLatencySensor = registerSensorIfAbsent(
        helixResourceCreationLatencySensorName,
        new Avg(),
        new Max(),
        TehutiUtils
            .getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + helixResourceCreationLatencySensorName));
  }

  public void recordRetireOldVersionsLatency(long latency) {
    retireOldVersionsLatencySensor.record(latency);
  }

  public void recordResourceAssignmentWaitLatency(long latency) {
    resourceAssignmentWaitLatencySensor.record(latency);
  }

  public void recordExistingSourceVersionHandlingLatency(long latency) {
    existingSourceVersionHandlingLatencySensor.record(latency);
  }

  public void recordStartOfPushLatency(long latency) {
    startOfPushLatencySensor.record(latency);
  }

  public void recordBatchTopicCreationLatency(long latency) {
    batchTopicCreationLatencySensor.record(latency);
  }

  public void recordHelixResourceCreationLatency(long latency) {
    helixResourceCreationLatencySensor.record(latency);
  }

  public void recordVersionCreationFailureLatency(long latency) {
    versionCreationFailureLatencySensor.record(latency);
  }
}
