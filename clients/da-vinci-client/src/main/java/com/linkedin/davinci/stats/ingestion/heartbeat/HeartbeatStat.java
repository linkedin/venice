package com.linkedin.davinci.stats.ingestion.heartbeat;

import com.linkedin.davinci.stats.WritePathLatencySensor;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.Set;


public class HeartbeatStat {
  Map<String, WritePathLatencySensor> readyToServeLeaderSensors = new VeniceConcurrentHashMap<>();
  Map<String, WritePathLatencySensor> readyToServeFollowerSensors = new VeniceConcurrentHashMap<>();
  Map<String, WritePathLatencySensor> catchingUpFollowerSensors = new VeniceConcurrentHashMap<>();

  // Record-level delay sensors only populated when record level config is enabled, otherwise it is the same as
  // heartbeat level
  Map<String, WritePathLatencySensor> readyToServeLeaderRecordSensors = new VeniceConcurrentHashMap<>();
  Map<String, WritePathLatencySensor> readyToServeFollowerRecordSensors = new VeniceConcurrentHashMap<>();
  Map<String, WritePathLatencySensor> catchingUpFollowerRecordSensors = new VeniceConcurrentHashMap<>();

  WritePathLatencySensor defaultSensor;

  public HeartbeatStat(MetricConfig metricConfig, Set<String> regions) {
    /**
     * Creating this separate local metric repository only to utilize the sensor library and not for reporting.
     * We report the value of these stats via HeartbeatStatsReporter which is a versioned via HeartbeatVersionedStats
     */
    MetricsRepository localRepository = new MetricsRepository(metricConfig);
    for (String region: regions) {
      // Heartbeat message sensors
      readyToServeLeaderSensors
          .put(region, new WritePathLatencySensor(localRepository, metricConfig, "leader-" + region));
      readyToServeFollowerSensors
          .put(region, new WritePathLatencySensor(localRepository, metricConfig, "follower-" + region));
      catchingUpFollowerSensors
          .put(region, new WritePathLatencySensor(localRepository, metricConfig, "catching-up-follower-" + region));

      // Record-level delay sensors
      readyToServeLeaderRecordSensors
          .put(region, new WritePathLatencySensor(localRepository, metricConfig, "leader-record-" + region));
      readyToServeFollowerRecordSensors
          .put(region, new WritePathLatencySensor(localRepository, metricConfig, "follower-record-" + region));
      catchingUpFollowerRecordSensors.put(
          region,
          new WritePathLatencySensor(localRepository, metricConfig, "catching-up-follower-record-" + region));
    }
    // This is an edge case return that should not happen, but it 'can' happen if a venice server is configured with no
    // local fabric in it's config. This currently isn't illegal, and probably wasn't made to be illegal so as to
    // preserve older behavior. TODO: remove this and make local fabric server name a required config
    defaultSensor = new WritePathLatencySensor(localRepository, metricConfig, "default-");
  }

  /**
   * Records the heartbeat lag for a ready-to-serve leader replica.
   *
   * @param region The region name
   * @param delay The pre-calculated delay in milliseconds
   * @param endTime The pre-calculated end time
   */
  public void recordReadyToServeLeaderLag(String region, long delay, long endTime) {
    readyToServeLeaderSensors.computeIfAbsent(region, k -> defaultSensor).record(delay, endTime);
  }

  /**
   * Records the heartbeat lag for a ready-to-serve follower replica.
   *
   * @param region The region name
   * @param delay The pre-calculated delay in milliseconds
   * @param endTime The pre-calculated end time
   */
  public void recordReadyToServeFollowerLag(String region, long delay, long endTime) {
    readyToServeFollowerSensors.computeIfAbsent(region, k -> defaultSensor).record(delay, endTime);
  }

  /**
   * Records the heartbeat lag for a catching-up follower replica.
   *
   * @param region The region name
   * @param delay The pre-calculated delay in milliseconds (0 for squelching)
   * @param endTime The pre-calculated end time
   */
  public void recordCatchingUpFollowerLag(String region, long delay, long endTime) {
    catchingUpFollowerSensors.computeIfAbsent(region, k -> defaultSensor).record(delay, endTime);
  }

  public WritePathLatencySensor getReadyToServeLeaderLag(String region) {
    return readyToServeLeaderSensors.computeIfAbsent(region, k -> defaultSensor);
  }

  public WritePathLatencySensor getReadyToServeFollowerLag(String region) {
    return readyToServeFollowerSensors.computeIfAbsent(region, k -> defaultSensor);
  }

  public WritePathLatencySensor getCatchingUpFollowerLag(String region) {
    return catchingUpFollowerSensors.computeIfAbsent(region, k -> defaultSensor);
  }

  /**
   * Records the record-level delay for a ready-to-serve leader replica.
   *
   * @param region The region name
   * @param delay The pre-calculated delay in milliseconds
   * @param endTime The pre-calculated end time
   */
  public void recordReadyToServeLeaderRecordLag(String region, long delay, long endTime) {
    readyToServeLeaderRecordSensors.computeIfAbsent(region, k -> defaultSensor).record(delay, endTime);
  }

  /**
   * Records the record-level delay for a ready-to-serve follower replica.
   *
   * @param region The region name
   * @param delay The pre-calculated delay in milliseconds
   * @param endTime The pre-calculated end time
   */
  public void recordReadyToServeFollowerRecordLag(String region, long delay, long endTime) {
    readyToServeFollowerRecordSensors.computeIfAbsent(region, k -> defaultSensor).record(delay, endTime);
  }

  /**
   * Records the record-level delay for a catching-up follower replica.
   *
   * @param region The region name
   * @param delay The pre-calculated delay in milliseconds (0 for squelching)
   * @param endTime The pre-calculated end time
   */
  public void recordCatchingUpFollowerRecordLag(String region, long delay, long endTime) {
    catchingUpFollowerRecordSensors.computeIfAbsent(region, k -> defaultSensor).record(delay, endTime);
  }

  public WritePathLatencySensor getReadyToServeLeaderRecordLag(String region) {
    return readyToServeLeaderRecordSensors.computeIfAbsent(region, k -> defaultSensor);
  }

  public WritePathLatencySensor getReadyToServeFollowerRecordLag(String region) {
    return readyToServeFollowerRecordSensors.computeIfAbsent(region, k -> defaultSensor);
  }

  public WritePathLatencySensor getCatchingUpFollowerRecordLag(String region) {
    return catchingUpFollowerRecordSensors.computeIfAbsent(region, k -> defaultSensor);
  }
}
