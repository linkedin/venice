package com.linkedin.davinci.stats;

import com.linkedin.davinci.kafka.consumer.StoreBufferService;
import com.linkedin.davinci.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.writer.ApacheKafkaProducer;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;


public class AggLagStats extends AbstractVeniceStats {
  private static final Logger LOGGER = Logger.getLogger(AggLagStats.class);

  private final StoreIngestionService storeIngestionService;

  private long aggBatchReplicationLagFuture;
  private long aggLeaderOffsetLagFuture;
  private long aggFollowerOffsetLagFuture;

  private long lastLagUpdateTsMs = 0;

  public AggLagStats(StoreIngestionService storeIngestionService, MetricsRepository metricsRepository) {
    super(metricsRepository, "AggLagStats");
    this.storeIngestionService = storeIngestionService;

    registerSensor("agg_batch_replication_lag_future", new Gauge(
        () -> this.getAggBatchReplicationLagFuture()
    ));
    registerSensor("agg_leader_offset_lag_future", new Gauge(
        () -> this.getAggLeaderOffsetLagFuture()
    ));
    registerSensor("agg_follower_offset_lag_future", new Gauge(
        () -> this.getAggFollowerOffsetLagFuture()
    ));

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

    storeIngestionService.traverseAllIngestionTasksAndApply((ingestionTask) -> {
      if (ingestionTask.isFutureVersion()) {
        aggBatchReplicationLagFuture += ingestionTask.getBatchReplicationLag();
        aggLeaderOffsetLagFuture += ingestionTask.getBatchLeaderOffsetLag();
        aggFollowerOffsetLagFuture += ingestionTask.getBatchFollowerOffsetLag();
      }
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
}
