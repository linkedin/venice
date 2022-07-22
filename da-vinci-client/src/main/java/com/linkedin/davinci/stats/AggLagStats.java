package com.linkedin.davinci.stats;

import com.linkedin.davinci.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.RegionUtils;
import com.linkedin.venice.utils.Time;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;


public class AggLagStats extends AbstractVeniceStats {

  private final StoreIngestionService storeIngestionService;
  private final Map<Integer, String> kafkaClusterIdToAliasMap;
  private final Int2LongMap aggRegionHybridOffsetLagTotalMap;

  private long aggBatchReplicationLagFuture;
  private long aggBatchLeaderOffsetLagFuture;
  private long aggBatchFollowerOffsetLagFuture;
  private long aggHybridLeaderOffsetLagTotal;
  private long aggHybridFollowerOffsetLagTotal;
  private long lastLagUpdateTsMs = 0;

  public AggLagStats(StoreIngestionService storeIngestionService, MetricsRepository metricsRepository) {
    super(metricsRepository, "AggLagStats");
    this.storeIngestionService = storeIngestionService;
    this.kafkaClusterIdToAliasMap = storeIngestionService.getVeniceConfigLoader().getVeniceServerConfig().getKafkaClusterIdToAliasMap();
    this.aggRegionHybridOffsetLagTotalMap = new Int2LongOpenHashMap(kafkaClusterIdToAliasMap.size());
    for (Map.Entry<Integer, String> entry : kafkaClusterIdToAliasMap.entrySet()) {
      String regionNamePrefix = RegionUtils.getRegionSpecificMetricPrefix(storeIngestionService.getVeniceConfigLoader().getVeniceServerConfig().getRegionName(), entry.getValue());
      registerSensor(regionNamePrefix + "_rt_lag", new Gauge(() -> getAggRegionHybridOffsetLagTotal(entry.getKey())));
    }
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
    aggRegionHybridOffsetLagTotalMap.clear();

    storeIngestionService.traverseAllIngestionTasksAndApply((ingestionTask) -> {
      if (ingestionTask.isFutureVersion()) {
        aggBatchReplicationLagFuture += ingestionTask.getBatchReplicationLag();
        aggBatchLeaderOffsetLagFuture += ingestionTask.getBatchLeaderOffsetLag();
        aggBatchFollowerOffsetLagFuture += ingestionTask.getBatchFollowerOffsetLag();
      }

      aggHybridLeaderOffsetLagTotal += ingestionTask.getHybridLeaderOffsetLag();
      aggHybridFollowerOffsetLagTotal += ingestionTask.getHybridFollowerOffsetLag();
    });

    for (int regionId : kafkaClusterIdToAliasMap.keySet()) {
      AtomicLong totalValue = new AtomicLong();
      storeIngestionService.traverseAllIngestionTasksAndApply((ingestionTask) -> {
        totalValue.addAndGet(ingestionTask.getRegionHybridOffsetLag(regionId));
      });
      aggRegionHybridOffsetLagTotalMap.put(regionId, totalValue.longValue());
    }

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

  public long getAggRegionHybridOffsetLagTotal(int regionId) {
    mayCollectAllLags();
    return aggRegionHybridOffsetLagTotalMap.getOrDefault(regionId, 0L);
  }
}
