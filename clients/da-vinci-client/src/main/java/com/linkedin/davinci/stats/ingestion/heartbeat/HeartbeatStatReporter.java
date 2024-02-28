package com.linkedin.davinci.stats.ingestion.heartbeat;

import com.linkedin.davinci.stats.AbstractVeniceStatsReporter;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.Set;


public class HeartbeatStatReporter extends AbstractVeniceStatsReporter<HeartbeatStat> {
  private static final String LEADER_METRIC_PREFIX = "heartbeat_delay_ms_leader-";
  private static final String FOLLOWER_METRIC_PREFIX = "heartbeat_delay_ms_follower-";
  private static final String MAX = ".Max";
  private static final String AVG = ".Avg";

  public HeartbeatStatReporter(MetricsRepository metricsRepository, String storeName, Set<String> regions) {
    super(metricsRepository, storeName);
    for (String region: regions) {
      registerSensor(
          new AsyncGauge(
              (ignored, ignored2) -> getStats().getLeaderLag(region).getMax(),
              LEADER_METRIC_PREFIX + region + MAX));
      registerSensor(
          new AsyncGauge(
              (ignored, ignored2) -> getStats().getFollowerLag(region).getMax(),
              FOLLOWER_METRIC_PREFIX + region + MAX));
      registerSensor(
          new AsyncGauge(
              (ignored, ignored2) -> getStats().getLeaderLag(region).getAvg(),
              LEADER_METRIC_PREFIX + region + AVG));
      registerSensor(
          new AsyncGauge(
              (ignored, ignored2) -> getStats().getFollowerLag(region).getAvg(),
              FOLLOWER_METRIC_PREFIX + region + AVG));
    }
  }

  @Override
  protected void registerStats() {
    // NoOp
  }
}
