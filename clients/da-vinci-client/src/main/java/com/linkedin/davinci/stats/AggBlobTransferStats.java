package com.linkedin.davinci.stats;

/**
 * Aggregates blob transfer statistics across both versioned and host-level metrics.
 * This class serves as a facade that coordinates recording blob transfer events to both
 * version-specific statistics and host-level aggregated statistics.
 */
public class AggBlobTransferStats {
  private final AggVersionedBlobTransferStats aggVersionedBlobTransferStats;
  private final AggHostLevelIngestionStats aggHostLevelIngestionStats;

  /**
   * Constructs an AggBlobTransferStats instance.
   *
   * @param aggVersionedBlobTransferStats the versioned blob transfer statistics aggregator
   * @param aggHostLevelIngestionStats the host-level ingestion statistics aggregator
   */
  public AggBlobTransferStats(
      AggVersionedBlobTransferStats aggVersionedBlobTransferStats,
      AggHostLevelIngestionStats aggHostLevelIngestionStats) {
    this.aggHostLevelIngestionStats = aggHostLevelIngestionStats;
    this.aggVersionedBlobTransferStats = aggVersionedBlobTransferStats;
  }

  /**
   * Records the number of bytes sent during a blob transfer operation.
   * This method updates both version-specific and host-level statistics.
   *
   * @param storeName the name of the Venice store
   * @param version the version number of the store
   * @param value the number of bytes sent
   */
  public void recordBlobTransferBytesSent(String storeName, int version, long value) {
    aggVersionedBlobTransferStats.recordBlobTransferBytesSent(storeName, version, value);
    HostLevelIngestionStats totalStats = aggHostLevelIngestionStats.getTotalStats();
    if (totalStats != null) {
      totalStats.recordTotalBlobTransferBytesSend(value);
    }
  }

  /**
   * Records the number of bytes received during a blob transfer operation.
   * This method updates both version-specific and host-level statistics.
   *
   * @param storeName the name of the Venice store
   * @param version the version number of the store
   * @param value the number of bytes received
   */
  public void recordBlobTransferBytesReceived(String storeName, int version, long value) {
    aggVersionedBlobTransferStats.recordBlobTransferBytesReceived(storeName, version, value);
    HostLevelIngestionStats totalStats = aggHostLevelIngestionStats.getTotalStats();
    if (totalStats != null) {
      totalStats.recordTotalBlobTransferBytesReceived(value);
    }
  }

  /**
   * Returns the versioned blob transfer statistics aggregator.
   *
   * @return the {@link AggVersionedBlobTransferStats} instance
   */
  public AggVersionedBlobTransferStats getAggVersionedBlobTransferStats() {
    return aggVersionedBlobTransferStats;
  }
}
