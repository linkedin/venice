package com.linkedin.davinci.stats;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.stats.dimensions.VeniceBlobTransferFallbackReason;
import com.linkedin.venice.stats.dimensions.VeniceBlobTransferSource;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.utils.Time;
import io.tehuti.metrics.MetricsRepository;


/**
 * Aggregates blob transfer statistics at the store version level.
 * This class manages versioned statistics for blob transfer operations, tracking metrics such as
 * response counts, throughput, transfer times, and bytes sent/received for each store version.
 * It extends {@link AbstractVeniceAggVersionedStats} to provide automatic aggregation across
 * all versions of a store.
 *
 * <p><b>OTel stats lifecycle:</b> OTel stats are created lazily by {@link #getBlobTransferOtelStats};
 * the shared per-store registry updates their version info and closes them on store deletion.
 */
public class AggVersionedBlobTransferStats
    extends AbstractVeniceAggVersionedStats<BlobTransferStats, BlobTransferStatsReporter> {
  private final PerStoreVersionedOtelStats<BlobTransferOtelStats> otelStats;
  private final String clusterName;

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
    this.clusterName = serverConfig.getClusterName();
    this.otelStats = createOtelStats();
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
    this.clusterName = serverConfig.getClusterName();
    this.otelStats = createOtelStats();
  }

  private PerStoreVersionedOtelStats<BlobTransferOtelStats> createOtelStats() {
    return createPerStoreOtelStats(
        storeName -> new BlobTransferOtelStats(getMetricsRepository(), storeName, clusterName));
  }

  private BlobTransferOtelStats getBlobTransferOtelStats(String storeName) {
    return otelStats.getOrCreate(storeName);
  }

  /**
   * Record the blob transfer request count (Tehuti only).
   *
   * <p>OTel metrics are intentionally NOT recorded here to avoid double-counting.
   * OTel uses a single counter with a {@link VeniceResponseStatusCategory} dimension,
   * recorded by {@link #recordBlobTransferResponsesBasedOnBoostrapStatus} instead.
   * The total is the sum of SUCCESS + FAIL in OTel.
   */
  public void recordBlobTransferResponsesCount(String storeName, int version) {
    // Tehuti metrics only
    recordVersionedAndTotalStat(storeName, version, BlobTransferStats::recordBlobTransferResponsesCount);
  }

  /**
   * Records the blob transfer request count based on the bootstrap status (Tehuti and OTel).
   *
   * @param storeName the store name
   * @param version the version of the store
   * @param isBlobTransferSuccess true if the blob transfer is successful, false otherwise
   */
  public void recordBlobTransferResponsesBasedOnBoostrapStatus(
      String storeName,
      int version,
      boolean isBlobTransferSuccess) {
    // Tehuti metrics
    recordVersionedAndTotalStat(
        storeName,
        version,
        stats -> stats.recordBlobTransferResponsesBasedOnBoostrapStatus(isBlobTransferSuccess));
    // OTel metrics
    getBlobTransferOtelStats(storeName).recordResponseCount(
        version,
        isBlobTransferSuccess ? VeniceResponseStatusCategory.SUCCESS : VeniceResponseStatusCategory.FAIL);
  }

  /**
   * Records an attempted blob transfer, attributed to its source (Tehuti and OTel).
   *
   * @param storeName the store name
   * @param version the version of the store
   * @param source the peer type that served the transfer attempt
   * @param status SUCCESS if the blob was fetched, FAIL otherwise
   */
  public void recordBlobTransferRequest(
      String storeName,
      int version,
      VeniceBlobTransferSource source,
      VeniceResponseStatusCategory status) {
    // Tehuti metrics
    recordVersionedAndTotalStat(storeName, version, stats -> stats.recordBlobTransferRequest(source, status));
    // OTel metrics
    getBlobTransferOtelStats(storeName).recordRequestCount(version, source, status);
  }

  /**
   * Records that a replica bootstrapped from the version topic instead of blob transfer (Tehuti and OTel).
   *
   * @param storeName the store name
   * @param version the version of the store
   * @param reason why blob transfer was not used
   */
  public void recordBlobTransferVersionTopicFallback(
      String storeName,
      int version,
      VeniceBlobTransferFallbackReason reason) {
    // Tehuti metrics
    recordVersionedAndTotalStat(storeName, version, stats -> stats.recordBlobTransferVersionTopicFallback(reason));
    // OTel metrics
    getBlobTransferOtelStats(storeName).recordVersionTopicFallback(version, reason);
  }

  /**
   * Record the blob transfer file receive throughput (Tehuti only).
   *
   * <p>OTel does not have a separate throughput metric — throughput is derivable
   * as the rate of {@code bytes.received}.
   */
  public void recordBlobTransferFileReceiveThroughput(String storeName, int version, double throughput) {
    // Tehuti metrics only
    recordVersionedAndTotalStat(storeName, version, stats -> stats.recordBlobTransferFileReceiveThroughput(throughput));
  }

  /**
   * Records the blob transfer time (Tehuti and OTel).
   *
   * @param storeName the store name
   * @param version the version of the store
   * @param timeInSec the time in seconds
   */
  public void recordBlobTransferTimeInSec(String storeName, int version, double timeInSec) {
    // Tehuti metrics
    recordVersionedAndTotalStat(storeName, version, stats -> stats.recordBlobTransferTimeInSec(timeInSec));
    // OTel metrics
    getBlobTransferOtelStats(storeName).recordTime(version, timeInSec);
  }

  /**
   * Records the number of bytes received during a blob transfer operation (Tehuti and OTel).
   *
   * @param storeName the name of the Venice store
   * @param version the version number of the store
   * @param value the number of bytes received
   */
  public void recordBlobTransferBytesReceived(String storeName, int version, long value) {
    // Tehuti metrics
    recordVersionedAndTotalStat(storeName, version, stats -> stats.recordBlobTransferBytesReceived(value));
    // OTel metrics
    getBlobTransferOtelStats(storeName).recordBytesReceived(version, value);
  }

  /**
   * Records the number of bytes sent during a blob transfer operation (Tehuti and OTel).
   *
   * @param storeName the name of the Venice store
   * @param version the version number of the store
   * @param value the number of bytes sent
   */
  public void recordBlobTransferBytesSent(String storeName, int version, long value) {
    // Tehuti metrics
    recordVersionedAndTotalStat(storeName, version, stats -> stats.recordBlobTransferBytesSent(value));
    // OTel metrics
    getBlobTransferOtelStats(storeName).recordBytesSent(version, value);
  }
}
