package com.linkedin.davinci.stats;

public class AggBlobTransferStats {
  private final AggVersionedBlobTransferStats aggVersionedBlobTransferStats;
  private final AggHostLevelIngestionStats aggHostLevelIngestionStats;

  public AggBlobTransferStats(
      AggVersionedBlobTransferStats aggVersionedBlobTransferStats,
      AggHostLevelIngestionStats aggHostLevelIngestionStats) {
    this.aggHostLevelIngestionStats = aggHostLevelIngestionStats;
    this.aggVersionedBlobTransferStats = aggVersionedBlobTransferStats;
  }

  public void recordBlobTransferBytesSent(String storeName, int version, long value) {
    aggVersionedBlobTransferStats.recordBlobTransferBytesSent(storeName, version, value);
    HostLevelIngestionStats totalStats = aggHostLevelIngestionStats.getTotalStats();
    if (totalStats != null) {
      totalStats.recordTotalBlobTransferBytesSend(value);
    }
  }

  public void recordBlobTransferBytesReceived(String storeName, int version, long value) {
    aggVersionedBlobTransferStats.recordBlobTransferBytesReceived(storeName, version, value);
    HostLevelIngestionStats totalStats = aggHostLevelIngestionStats.getTotalStats();
    if (totalStats != null) {
      totalStats.recordTotalBlobTransferBytesReceived(value);
    }
  }

  public AggVersionedBlobTransferStats getAggVersionedBlobTransferStats() {
    return aggVersionedBlobTransferStats;
  }
}
