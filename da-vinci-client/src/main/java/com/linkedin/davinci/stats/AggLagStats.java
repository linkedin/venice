package com.linkedin.davinci.stats;

import com.linkedin.davinci.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Time;
import io.tehuti.metrics.MetricsRepository;
import org.apache.log4j.Logger;


public class AggLagStats extends AbstractVeniceStats {
  private static final Logger LOGGER = Logger.getLogger(AggLagStats.class);

  private final StoreIngestionService storeIngestionService;

  private long aggBatchReplicationLagFuture;
  private long aggBatchLeaderOffsetLagFuture;
  private long aggBatchFollowerOffsetLagFuture;
  private long aggHybridLeaderOffsetLagTotal;
  private long aggHybridFollowerOffsetLagTotal;

  private long lastLagUpdateTsMs = 0;

  public AggLagStats(StoreIngestionService storeIngestionService, MetricsRepository metricsRepository) {
    super(metricsRepository, "AggLagStats");
    this.storeIngestionService = storeIngestionService;

    registerSensor("agg_batch_replication_lag_future", new Gauge(this::getAggBatchReplicationLagFuture));
    registerSensor("agg_batch_leader_offset_lag_future", new Gauge(this::getAggBatchLeaderOffsetLagFuture));
    registerSensor("agg_batch_follower_offset_lag_future", new Gauge(this::getAggBatchFollowerOffsetLagFuture));
    registerSensor("agg_hybrid_leader_offset_lag_total", new Gauge(this::getAggHybridLeaderOffsetLagTotal));
    registerSensor("agg_hybrid_follower_offset_lag_total", new Gauge(this::getAggHybridFollowerOffsetLagTotal));
  }

  private synchronized void mayCollectAllLags() {
    /**
     * Will cache the result for 60 seconds to avoid looping through all ingestion task every time.
     */
    if (LatencyUtils.getElapsedTimeInMs(lastLagUpdateTsMs) < 60 * Time.MS_PER_SECOND) {
      return;
    }
    aggBatchReplicationLagFuture = 0;
    aggBatchLeaderOffsetLagFuture = 0;
    aggBatchFollowerOffsetLagFuture = 0;
    aggHybridLeaderOffsetLagTotal = 0;
    aggHybridFollowerOffsetLagTotal = 0;

    storeIngestionService.traverseAllIngestionTasksAndApply((ingestionTask) -> {
      if (ingestionTask.isFutureVersion()) {
        aggBatchReplicationLagFuture += ingestionTask.getBatchReplicationLag();
        aggBatchLeaderOffsetLagFuture += ingestionTask.getBatchLeaderOffsetLag();
        aggBatchFollowerOffsetLagFuture += ingestionTask.getBatchFollowerOffsetLag();
      }

      aggHybridLeaderOffsetLagTotal += ingestionTask.getHybridLeaderOffsetLag();
      aggHybridFollowerOffsetLagTotal += ingestionTask.getHybridFollowerOffsetLag();
    });

    lastLagUpdateTsMs = System.currentTimeMillis();
  }

  public long getAggBatchReplicationLagFuture() {
    mayCollectAllLags();
    return aggBatchReplicationLagFuture;
  }

  public long getAggBatchLeaderOffsetLagFuture() {
    mayCollectAllLags();
    return aggBatchLeaderOffsetLagFuture;
  }

  public long getAggBatchFollowerOffsetLagFuture() {
    mayCollectAllLags();
    return aggBatchFollowerOffsetLagFuture;
  }

  public long getAggHybridLeaderOffsetLagTotal() {
    mayCollectAllLags();
    return aggHybridLeaderOffsetLagTotal;
  }

  public long getAggHybridFollowerOffsetLagTotal() {
    mayCollectAllLags();
    return aggHybridFollowerOffsetLagTotal;
  }
}
