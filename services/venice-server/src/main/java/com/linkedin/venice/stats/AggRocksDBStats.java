package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import org.rocksdb.Statistics;


/**
 * Right now, Venice SN only reports aggregated metrics for RocksDB.
 */
public class AggRocksDBStats extends AbstractVeniceAggStats<RocksDBStats> {
  public AggRocksDBStats(String cluster, MetricsRepository metricsRepository, Statistics aggStat) {
    super(
        cluster,
        metricsRepository,
        (metricsRepo, storeName, clusterName) -> new RocksDBStats(metricsRepository, storeName),
        false);
    totalStats.setRocksDBStat(aggStat);
  }
}
