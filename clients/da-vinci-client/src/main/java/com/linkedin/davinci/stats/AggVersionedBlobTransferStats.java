package com.linkedin.davinci.stats;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import io.tehuti.metrics.MetricsRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The store level stats for blob transfer
 */
public class AggVersionedBlobTransferStats
    extends AbstractVeniceAggVersionedStats<BlobTransferStats, BlobTransferStatsReporter> {
  private static final Logger LOGGER = LogManager.getLogger(AggVersionedBlobTransferStats.class);

  public AggVersionedBlobTransferStats(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      VeniceServerConfig serverConfig) {
    super(
        metricsRepository,
        metadataRepository,
        () -> new BlobTransferStats(),
        BlobTransferStatsReporter::new,
        serverConfig.isUnregisterMetricForDeletedStoreEnabled());
  }

  /**
   * Record the blob transfer request count
   * @param storeName
   * @param version
   */
  public void recordBlobTransferResponsesCount(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, stats -> stats.recordBlobTransferResponsesCount());
  }

  /**
   * Record the blob transfer request count based on the bootstrap status
   * @param storeName the store name
   * @param version the version of the store
   * @param isBlobTransferSuccess true if the blob transfer is successful, false otherwise
   */
  public void recordBlobTransferResponsesBasedOnBoostrapStatus(
      String storeName,
      int version,
      boolean isBlobTransferSuccess) {
    recordVersionedAndTotalStat(
        storeName,
        version,
        stats -> stats.recordBlobTransferResponsesBasedOnBoostrapStatus(isBlobTransferSuccess));
  }

  /**
   * Record the blob transfer file send throughput
   * @param storeName the store name
   * @param version the version of the store
   * @param throughput the throughput in MB/sec
   */
  public void recordBlobTransferFileReceiveThroughput(String storeName, int version, double throughput) {
    recordVersionedAndTotalStat(storeName, version, stats -> stats.recordBlobTransferFileReceiveThroughput(throughput));
  }

  /**
   * Record the blob transfer file receive throughput
   * @param storeName the store name
   * @param version the version of the store
   * @param timeInSec the time in seconds
   */
  public void recordBlobTransferTimeInSec(String storeName, int version, double timeInSec) {
    recordVersionedAndTotalStat(storeName, version, stats -> stats.recordBlobTransferTimeInSec(timeInSec));
  }
}
