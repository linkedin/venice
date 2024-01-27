package com.linkedin.davinci.stats.ingestion.heartbeat;

import com.linkedin.davinci.stats.AbstractVeniceStatsReporter;
import com.linkedin.venice.stats.Gauge;
import io.tehuti.metrics.MetricsRepository;
import java.util.Set;


public class HeartbeatStatReporter extends AbstractVeniceStatsReporter<HeartbeatStat> {
  private static final String LEADER_METRIC_PREFIX = "--heartbeat_delay_leader-";
  private static final String FOLLOWER_METRIC_PREFIX = "--heartbeat_delay_follower-";
  private static final String MAX = ".Max";
  private static final String AVG = ".Avg";

  public HeartbeatStatReporter(MetricsRepository metricsRepository, String storeName, Set<String> regions) {
    super(metricsRepository, storeName);
    for (String region: regions) {
      registerSensor(LEADER_METRIC_PREFIX + region + MAX, new Gauge(() -> getStats().getLeaderLag(region).getMax()));
      registerSensor(
          FOLLOWER_METRIC_PREFIX + region + MAX,
          new Gauge(() -> getStats().getFollowerLag(region).getMax()));
      registerSensor(LEADER_METRIC_PREFIX + region + AVG, new Gauge(() -> getStats().getLeaderLag(region).getAvg()));
      registerSensor(
          FOLLOWER_METRIC_PREFIX + region + AVG,
          new Gauge(() -> getStats().getFollowerLag(region).getAvg()));
    }
  }

  @Override
  protected void registerStats() {
    // NoOp
  }
}
