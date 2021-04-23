package com.linkedin.davinci.stats;

import com.linkedin.davinci.kafka.consumer.StoreBufferService;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


public class AggLagStats extends AbstractVeniceStats {

  private final AtomicLong aggBatchReplicationLagFuture;
  private final AtomicLong aggLeaderOffsetLagFuture;
  private final AtomicLong aggFollowerOffsetLagFuture;

  public AggLagStats(MetricsRepository metricsRepository) {
    super(metricsRepository, "AggLagStats");

    aggBatchReplicationLagFuture = new AtomicLong();
    aggLeaderOffsetLagFuture = new AtomicLong();
    aggFollowerOffsetLagFuture = new AtomicLong();

    /**
     * The following aggregated lag metrics depends on the corresponding store level metrics to work. It does not loop over
     * all store's lag metrics by itself. Instead it depends on the those metrics collection to happen
     * periodically for each store which contributes to these aggregated metrics.
     * If we stop producing the store level metrics the these metrics need to be refactored.
     */
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

  public long getAggBatchReplicationLagFuture() {
    long lag = aggBatchReplicationLagFuture.getAndSet(0);
    return lag;
  }

  public void addAggBatchReplicationLagFuture(long lag) {
    aggBatchReplicationLagFuture.addAndGet(lag);
  }

  public long getAggLeaderOffsetLagFuture() {
    long lag = aggLeaderOffsetLagFuture.getAndSet(0);
    return lag;
  }

  public void addAggLeaderOffsetLagFuture(long lag) {
    aggLeaderOffsetLagFuture.addAndGet(lag);
  }

  public long getAggFollowerOffsetLagFuture() {
    long lag = aggFollowerOffsetLagFuture.getAndSet(0);
    return lag;
  }

  public void addAggFollowerOffsetLagFuture(long lag) {
    aggFollowerOffsetLagFuture.addAndGet(lag);
  }


}
