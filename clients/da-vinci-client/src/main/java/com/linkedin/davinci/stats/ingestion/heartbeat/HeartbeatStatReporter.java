package com.linkedin.davinci.stats.ingestion.heartbeat;

import static com.linkedin.venice.stats.StatsErrorCode.NULL_INGESTION_STATS;

import com.linkedin.davinci.stats.AbstractVeniceStatsReporter;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.Set;


public class HeartbeatStatReporter extends AbstractVeniceStatsReporter<HeartbeatStat> {
  static final String LEADER_METRIC_PREFIX = "heartbeat_delay_ms_leader-";
  static final String FOLLOWER_METRIC_PREFIX = "heartbeat_delay_ms_follower-";
  static final String CATCHUP_UP_FOLLOWER_METRIC_PREFIX = "catching_up_heartbeat_delay_ms_follower-";

  // Record-level delay metric prefixes
  static final String LEADER_RECORD_METRIC_PREFIX = "record_delay_ms_leader-";
  static final String FOLLOWER_RECORD_METRIC_PREFIX = "record_delay_ms_follower-";
  static final String CATCHUP_UP_FOLLOWER_RECORD_METRIC_PREFIX = "catching_up_record_delay_ms_follower-";

  static final String MAX = "-Max";
  static final String AVG = "-Avg";

  public HeartbeatStatReporter(MetricsRepository metricsRepository, String storeName, Set<String> regions) {
    super(metricsRepository, storeName);
    for (String region: regions) {
      // Heartbeat message metrics
      registerSensor(new AsyncGauge((ignored, ignored2) -> {
        if (getStats() == null) {
          return NULL_INGESTION_STATS.code;
        }
        return getStats().getReadyToServeLeaderLag(region).getMax();
      }, LEADER_METRIC_PREFIX + region + MAX));

      registerSensor(new AsyncGauge((ignored, ignored2) -> {
        if (getStats() == null) {
          return NULL_INGESTION_STATS.code;
        }
        return getStats().getReadyToServeLeaderLag(region).getAvg();
      }, LEADER_METRIC_PREFIX + region + AVG));

      // Record-level delay metrics for leader
      registerSensor(new AsyncGauge((ignored, ignored2) -> {
        if (getStats() == null) {
          return NULL_INGESTION_STATS.code;
        }
        return getStats().getReadyToServeLeaderRecordLag(region).getMax();
      }, LEADER_RECORD_METRIC_PREFIX + region + MAX));

      registerSensor(new AsyncGauge((ignored, ignored2) -> {
        if (getStats() == null) {
          return NULL_INGESTION_STATS.code;
        }
        return getStats().getReadyToServeLeaderRecordLag(region).getAvg();
      }, LEADER_RECORD_METRIC_PREFIX + region + AVG));

      // Do not register follower heartbeat metrics for separate RT region.
      if (Utils.isSeparateTopicRegion(region)) {
        continue;
      }

      // Heartbeat message metrics for follower
      registerSensor(new AsyncGauge((ignored, ignored2) -> {
        if (getStats() == null) {
          return NULL_INGESTION_STATS.code;
        }
        return getStats().getReadyToServeFollowerLag(region).getMax();
      }, FOLLOWER_METRIC_PREFIX + region + MAX));

      registerSensor(new AsyncGauge((ignored, ignored2) -> {
        if (getStats() == null) {
          return NULL_INGESTION_STATS.code;
        }
        return getStats().getReadyToServeFollowerLag(region).getAvg();
      }, FOLLOWER_METRIC_PREFIX + region + AVG));

      registerSensor(new AsyncGauge((ignored, ignored2) -> {
        if (getStats() == null) {
          return NULL_INGESTION_STATS.code;
        }
        return getStats().getCatchingUpFollowerLag(region).getMax();
      }, CATCHUP_UP_FOLLOWER_METRIC_PREFIX + region + MAX));

      registerSensor(new AsyncGauge((ignored, ignored2) -> {
        if (getStats() == null) {
          return NULL_INGESTION_STATS.code;
        }
        return getStats().getCatchingUpFollowerLag(region).getAvg();
      }, CATCHUP_UP_FOLLOWER_METRIC_PREFIX + region + AVG));

      // Record-level delay metrics for follower
      registerSensor(new AsyncGauge((ignored, ignored2) -> {
        if (getStats() == null) {
          return NULL_INGESTION_STATS.code;
        }
        return getStats().getReadyToServeFollowerRecordLag(region).getMax();
      }, FOLLOWER_RECORD_METRIC_PREFIX + region + MAX));

      registerSensor(new AsyncGauge((ignored, ignored2) -> {
        if (getStats() == null) {
          return NULL_INGESTION_STATS.code;
        }
        return getStats().getReadyToServeFollowerRecordLag(region).getAvg();
      }, FOLLOWER_RECORD_METRIC_PREFIX + region + AVG));

      registerSensor(new AsyncGauge((ignored, ignored2) -> {
        if (getStats() == null) {
          return NULL_INGESTION_STATS.code;
        }
        return getStats().getCatchingUpFollowerRecordLag(region).getMax();
      }, CATCHUP_UP_FOLLOWER_RECORD_METRIC_PREFIX + region + MAX));

      registerSensor(new AsyncGauge((ignored, ignored2) -> {
        if (getStats() == null) {
          return NULL_INGESTION_STATS.code;
        }
        return getStats().getCatchingUpFollowerRecordLag(region).getAvg();
      }, CATCHUP_UP_FOLLOWER_RECORD_METRIC_PREFIX + region + AVG));
    }
  }

  @Override
  protected void registerStats() {
    // NoOp
  }
}
