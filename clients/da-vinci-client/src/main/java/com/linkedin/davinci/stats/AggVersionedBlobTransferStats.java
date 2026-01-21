package com.linkedin.davinci.stats;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.utils.Time;
import io.tehuti.metrics.MetricsRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Aggregates blob transfer statistics at the store version level.
 * This class manages versioned statistics for blob transfer operations, tracking metrics such as
 * response counts, throughput, transfer times, and bytes sent/received for each store version.
 * It extends {@link AbstractVeniceAggVersionedStats} to provide automatic aggregation across
 * all versions of a store.
 */
public class AggVersionedBlobTransferStats
    extends AbstractVeniceAggVersionedStats<BlobTransferStats, BlobTransferStatsReporter> {
  private static final Logger LOGGER = LogManager.getLogger(AggVersionedBlobTransferStats.class);

  /**
   * Constructs an AggVersionedBlobTransferStats instance.
   *
   * @param metricsRepository the metrics repository for recording statistics
   * @param metadataRepository the store metadata repository
   * @param serverConfig the Venice server configuration
   */
  public AggVersionedBlobTransferStats(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      VeniceServerConfig serverConfig) {
    super(
        metricsRepository,
        metadataRepository,
        BlobTransferStats::new,
        BlobTransferStatsReporter::new,
        serverConfig.isUnregisterMetricForDeletedStoreEnabled());
  }

  /**
   * Constructor for testing that allows injecting a Time instance.
   *
   * @param metricsRepository the metrics repository for recording statistics
   * @param metadataRepository the store metadata repository
   * @param serverConfig the Venice server configuration
   * @param time the time instance for testing purposes
   */
  public AggVersionedBlobTransferStats(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      VeniceServerConfig serverConfig,
      Time time) {
    super(
        metricsRepository,
        metadataRepository,
        () -> new BlobTransferStats(time),
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

  /**
   * Records the number of bytes received during a blob transfer operation.
   *
   * @param storeName the name of the Venice store
   * @param version the version number of the store
   * @param value the number of bytes received
   */
  public void recordBlobTransferBytesReceived(String storeName, int version, long value) {
    recordVersionedAndTotalStat(storeName, version, stats -> stats.recordBlobTransferBytesReceived(value));
  }

  /**
   * Records the number of bytes sent during a blob transfer operation.
   *
   * @param storeName the name of the Venice store
   * @param version the version number of the store
   * @param value the number of bytes sent
   */
  public void recordBlobTransferBytesSent(String storeName, int version, long value) {
    recordVersionedAndTotalStat(storeName, version, stats -> stats.recordBlobTransferBytesSent(value));
  }

}
