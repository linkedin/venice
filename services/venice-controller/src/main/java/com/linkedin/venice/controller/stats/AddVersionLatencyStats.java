package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;


/**
 * This class is used to track the latency of various operations related to adding a version in Venice.
 *
 * It includes metrics for:
 * <ul>
 * <li>Handling add version requests when the source version exists</li>
 * <li>Creating batch topics (for both child and parent controllers)</li>
 * <li>Waiting for node resources assignment</li>
 * <li>Creating Helix storage cluster resources</li>
 * <li>Retiring old store versions</li>
 * <li>Sending the start of a push</li>
 * <li>Handling version creation failures</li>
 * </ul>
 * Each metric is using Milliseconds as the unit of measurement.
 */
public class AddVersionLatencyStats extends AbstractVeniceStats {
  private final Sensor retireOldStoreVersionsLatencySensor;
  private final Sensor waitTimeForResourcesAssignmentLatencySensor;
  private final Sensor handleVersionCreationFailureLatencySensor;
  private final Sensor handleAddVersionWithSourceVersionExistLatencySensor;
  private final Sensor sendStartOfPushLatencySensor;
  private final Sensor topicCreationLatencySensor;
  private final Sensor helixStorageClusterResourcesCreationLatencySensor;

  public AddVersionLatencyStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    retireOldStoreVersionsLatencySensor =
        registerSensorIfAbsent("add_version_retire_old_store_versions_latency", new Avg(), new Max());
    waitTimeForResourcesAssignmentLatencySensor =
        registerSensorIfAbsent("add_version_wait_time_for_resources_assignment_latency", new Avg(), new Max());
    handleVersionCreationFailureLatencySensor =
        registerSensorIfAbsent("add_version_handle_version_creation_failure_latency", new Avg(), new Max());
    handleAddVersionWithSourceVersionExistLatencySensor = registerSensorIfAbsent(
        "add_version_handle_add_version_with_source_version_exist_latency",
        new Avg(),
        new Max());
    sendStartOfPushLatencySensor =
        registerSensorIfAbsent("add_version_send_start_of_push_latency", new Avg(), new Max());
    topicCreationLatencySensor =
        registerSensorIfAbsent("add_version_create_batch_topics_latency", new Avg(), new Max());
    helixStorageClusterResourcesCreationLatencySensor =
        registerSensorIfAbsent("add_version_helix_storage_cluster_resources_creation_latency", new Avg(), new Max());
  }

  public void recordRetireOldStoreVersionsLatency(long latency) {
    retireOldStoreVersionsLatencySensor.record(latency);
  }

  public void recordWaitTimeForResourcesAssignmentLatency(long latency) {
    waitTimeForResourcesAssignmentLatencySensor.record(latency);
  }

  public void recordHandleAddVersionWithSourceVersionExistLatency(long latency) {
    handleAddVersionWithSourceVersionExistLatencySensor.record(latency);
  }

  public void recordSendStartOfPushLatency(long latency) {
    sendStartOfPushLatencySensor.record(latency);
  }

  public void recordCreateBatchTopicsLatency(long latency) {
    topicCreationLatencySensor.record(latency);
  }

  public void recordHelixStorageClusterResourcesCreationLatency(long latency) {
    helixStorageClusterResourcesCreationLatencySensor.record(latency);
  }

  public void recordHandleVersionCreationFailureLatency(long latency) {
    handleVersionCreationFailureLatencySensor.record(latency);
  }
}
