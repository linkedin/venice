package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import org.rocksdb.Statistics;


/**
 * Right now, Venice SN only reports aggregated metrics for RocksDB.
 */
public class AggRocksDBStats extends AbstractVeniceAggStats<RocksDBStats> {
  public AggRocksDBStats(MetricsRepository metricsRepository, Statistics aggStat) {
    super(metricsRepository, (metricsRepo, storeName) -> new RocksDBStats(metricsRepository, storeName));
    totalStats.setRocksDBStat(aggStat);
  }
}
