package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;


public class AddVersionLatencyStats extends AbstractVeniceStats {
  private final Sensor retiredVersionLatencySensor;
  private final Sensor waitTimeForResourcesSensor;
  private final Sensor handleVersionCreationFailureLatencySensor;
  private final Sensor handleAddVersionWithSourceVersionExistLatencySensor;
  private final Sensor sendStartOfPushLatencySensor;
  private final Sensor topicCreationLatencySensor;
  private final Sensor helixStorageClusterResourcesCreationLatencySensor;

  public AddVersionLatencyStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    retiredVersionLatencySensor = registerSensorIfAbsent("add_version_retired_version_latency", new Avg(), new Max());
    waitTimeForResourcesSensor = registerSensorIfAbsent("add_version_wait_time_for_resources", new Avg(), new Max());
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

  public void recordRetiredVersionLatency(long latency) {
    retiredVersionLatencySensor.record(latency);
  }

  public void recordWaitTimeForResources(long latency) {
    waitTimeForResourcesSensor.record(latency);
  }

  public void recordHandleVersionCreationFailureLatency(long latency) {
    handleVersionCreationFailureLatencySensor.record(latency);
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
}
