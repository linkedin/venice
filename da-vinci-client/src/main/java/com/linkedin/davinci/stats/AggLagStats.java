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
  private long aggLeaderOffsetLagFuture;
  private long aggFollowerOffsetLagFuture;
  private long aggHybridLeaderOffsetLag;
  private long aggHybridFollowerOffsetLag;

  private long lastLagUpdateTsMs = 0;

  public AggLagStats(StoreIngestionService storeIngestionService, MetricsRepository metricsRepository) {
    super(metricsRepository, "AggLagStats");
    this.storeIngestionService = storeIngestionService;

    registerSensor("agg_batch_replication_lag_future", new Gauge(this::getAggBatchReplicationLagFuture));
    registerSensor("agg_leader_offset_lag_future", new Gauge(this::getAggLeaderOffsetLagFuture));
    registerSensor("agg_follower_offset_lag_future", new Gauge(this::getAggFollowerOffsetLagFuture));
    registerSensor("agg_hybrid_leader_offset_lag", new Gauge(this::getAggHybridLeaderOffsetLag));
    registerSensor("agg_hybrid_follower_offset_lag", new Gauge(this::getAggHybridFollowerOffsetLag));
  }

  private synchronized void mayCollectAllLags() {
    /**
     * Will cache the result for 60 seconds to avoid looping through all ingestion task every time.
     */
    if (LatencyUtils.getElapsedTimeInMs(lastLagUpdateTsMs) < 60 * Time.MS_PER_SECOND) {
      return;
    }
    aggBatchReplicationLagFuture = 0;
    aggLeaderOffsetLagFuture = 0;
    aggFollowerOffsetLagFuture = 0;
    aggHybridLeaderOffsetLag = 0;
    aggHybridFollowerOffsetLag = 0;

    storeIngestionService.traverseAllIngestionTasksAndApply((ingestionTask) -> {
      if (ingestionTask.isFutureVersion()) {
        aggBatchReplicationLagFuture += ingestionTask.getBatchReplicationLag();
        aggLeaderOffsetLagFuture += ingestionTask.getBatchLeaderOffsetLag();
        aggFollowerOffsetLagFuture += ingestionTask.getBatchFollowerOffsetLag();
      }

      aggHybridLeaderOffsetLag += ingestionTask.getHybridLeaderOffsetLag();
      aggHybridFollowerOffsetLag += ingestionTask.getHybridFollowerOffsetLag();
    });

    lastLagUpdateTsMs = System.currentTimeMillis();
  }

  public long getAggBatchReplicationLagFuture() {
    mayCollectAllLags();
    return aggBatchReplicationLagFuture;
  }

  public long getAggLeaderOffsetLagFuture() {
    mayCollectAllLags();
    return aggLeaderOffsetLagFuture;
  }

  public long getAggFollowerOffsetLagFuture() {
    mayCollectAllLags();
    return aggFollowerOffsetLagFuture;
  }

  public long getAggHybridLeaderOffsetLag() {
    mayCollectAllLags();
    return aggHybridLeaderOffsetLag;
  }

  public long getAggHybridFollowerOffsetLag() {
    mayCollectAllLags();
    return aggHybridFollowerOffsetLag;
  }
}
