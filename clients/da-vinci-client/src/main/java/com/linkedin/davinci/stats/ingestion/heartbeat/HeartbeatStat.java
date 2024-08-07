package com.linkedin.davinci.stats.ingestion.heartbeat;

import com.linkedin.davinci.stats.WritePathLatencySensor;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.Set;


public class HeartbeatStat {
  Map<String, WritePathLatencySensor> leaderSensors = new VeniceConcurrentHashMap<>();
  Map<String, WritePathLatencySensor> followerSensors = new VeniceConcurrentHashMap<>();
  Lazy<WritePathLatencySensor> defaultSensor;

  public HeartbeatStat(MetricConfig metricConfig, Set<String> regions) {
    /**
     * Creating this separate local metric repository only to utilize the sensor library and not for reporting.
     * We report the value of these stats via HeartbeatStatsReporter which is a versioned via HeartbeatVersionedStats
     */
    MetricsRepository localRepository = new MetricsRepository(metricConfig);
    for (String region: regions) {
      leaderSensors.put(region, new WritePathLatencySensor(localRepository, metricConfig, "leader-" + region));
      followerSensors.put(region, new WritePathLatencySensor(localRepository, metricConfig, "follower-" + region));
    }
    // This is an edge case return that should not happen, but it 'can' happen if a venice server is configured with no
    // local fabric in it's config. This currently isn't illegal, and probably wasn't made to be illegal so as to
    // preserve older behavior. TODO: remove this and make local fabric server name a required config
    defaultSensor = Lazy.of(() -> new WritePathLatencySensor(localRepository, metricConfig, "default-"));
  }

  public void recordLeaderLag(String region, long startTime) {
    long endTime = System.currentTimeMillis();
    leaderSensors.computeIfAbsent(region, k -> defaultSensor.get()).record(endTime - startTime, endTime);
  }

  public void recordFollowerLag(String region, long startTime) {
    long endTime = System.currentTimeMillis();
    followerSensors.computeIfAbsent(region, k -> defaultSensor.get()).record(endTime - startTime, endTime);
  }

  public WritePathLatencySensor getLeaderLag(String region) {
    return leaderSensors.computeIfAbsent(region, k -> defaultSensor.get());
  }

  public WritePathLatencySensor getFollowerLag(String region) {
    return followerSensors.computeIfAbsent(region, k -> defaultSensor.get());
  }
}
