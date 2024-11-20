package com.linkedin.davinci.stats;

import com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferStatus;
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

  public void recordBlobTransferRequestsCount(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, stats -> stats.recordBlobTransferRequestsCount());
  }

  public void recordBlobTransferRequestsStatus(String storeName, int version, BlobTransferStatus status) {
    recordVersionedAndTotalStat(storeName, version, stats -> stats.recordBlobTransferRequestsStatus(status));
  }

  public void recordBlobTransferResponsesCount(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, stats -> stats.recordBlobTransferResponsesCount());
  }

  public void recordBlobTransferResponsesBasedOnBoostrapStatus(
      String storeName,
      int version,
      boolean isBlobTransferSuccess) {
    recordVersionedAndTotalStat(
        storeName,
        version,
        stats -> stats.recordBlobTransferResponsesBasedOnBoostrapStatus(isBlobTransferSuccess));
  }

  public void recordBlobTransferFileReceiveThroughput(String storeName, int version, double throughput) {
    recordVersionedAndTotalStat(storeName, version, stats -> stats.recordBlobTransferFileReceiveThroughput(throughput));
  }

  public void recordBlobTransferTimeInSec(String storeName, int version, double timeInSec) {
    recordVersionedAndTotalStat(storeName, version, stats -> stats.recordBlobTransferTimeInSec(timeInSec));
  }
}
