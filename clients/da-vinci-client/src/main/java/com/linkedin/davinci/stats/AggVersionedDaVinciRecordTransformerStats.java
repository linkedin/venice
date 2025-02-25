package com.linkedin.davinci.stats;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import io.tehuti.metrics.MetricsRepository;


/**
 * The store level stats for {@link com.linkedin.davinci.client.DaVinciRecordTransformer}
 */
public class AggVersionedDaVinciRecordTransformerStats
    extends AbstractVeniceAggVersionedStats<DaVinciRecordTransformerStats, DaVinciRecordTransformerStatsReporter> {
  public AggVersionedDaVinciRecordTransformerStats(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      VeniceServerConfig serverConfig) {
    super(
        metricsRepository,
        metadataRepository,
        DaVinciRecordTransformerStats::new,
        DaVinciRecordTransformerStatsReporter::new,
        serverConfig.isUnregisterMetricForDeletedStoreEnabled());
  }

  public void recordPutLatency(String storeName, int version, double value, long timestamp) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordPutLatency(value, timestamp));
  }

  public void recordDeleteLatency(String storeName, int version, double value, long timestamp) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordDeleteLatency(value, timestamp));
  }

  public void recordOnRecoveryLatency(String storeName, int version, double value, long timestamp) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordOnRecoveryLatency(value, timestamp));
  }

  public void recordOnStartVersionIngestionLatency(String storeName, int version, double value, long timestamp) {
    recordVersionedAndTotalStat(
        storeName,
        version,
        stat -> stat.recordOnStartVersionIngestionLatency(value, timestamp));
  }

  public void recordOnEndVersionIngestionLatency(String storeName, int version, double value, long timestamp) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordOnEndVersionIngestionLatency(value, timestamp));
  }

  public void recordPutError(String storeName, int version, long timestamp) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordPutError(timestamp));
  }

  public void recordDeleteError(String storeName, int version, long timestamp) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordDeleteError(timestamp));
  }
}
