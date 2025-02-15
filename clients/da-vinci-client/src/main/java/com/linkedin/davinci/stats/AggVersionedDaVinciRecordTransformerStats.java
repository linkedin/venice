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

  public void recordTransformerPutLatency(String storeName, int version, double value, long timestamp) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordTransformerPutLatency(value, timestamp));
  }

  public void recordTransformerDeleteLatency(String storeName, int version, double value, long timestamp) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordTransformerDeleteLatency(value, timestamp));
  }

  public void recordTransformerOnRecoveryLatency(String storeName, int version, double value, long timestamp) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordTransformerOnRecoveryLatency(value, timestamp));
  }

  public void recordTransformerOnStartVersionIngestionLatency(
      String storeName,
      int version,
      double value,
      long timestamp) {
    recordVersionedAndTotalStat(
        storeName,
        version,
        stat -> stat.recordTransformerOnStartVersionIngestionLatency(value, timestamp));
  }

  public void recordTransformerOnEndVersionIngestionLatency(
      String storeName,
      int version,
      double value,
      long timestamp) {
    recordVersionedAndTotalStat(
        storeName,
        version,
        stat -> stat.recordTransformerOnEndVersionIngestionLatency(value, timestamp));
  }

  public void recordTransformerPutError(String storeName, int version, double value, long timestamp) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordTransformerPutError(value, timestamp));
  }
}
