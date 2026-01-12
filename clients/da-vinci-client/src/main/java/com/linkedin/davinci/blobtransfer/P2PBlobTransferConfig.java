package com.linkedin.davinci.blobtransfer;

/**
 * All configs for P2P blob transfer.
 */
public class P2PBlobTransferConfig {
  // Port for P2P transfer server
  private final int p2pTransferServerPort;
  // Port for P2P transfer client, it should be same as the server port
  private final int p2pTransferClientPort;
  // Base directory for stores, snapshots, etc.
  private final String baseDir;
  // Max concurrent snapshot user, if the number of snapshot user exceeds this limit, the request will be rejected.
  private final int maxConcurrentSnapshotUser;
  // Snapshot retention time in minutes, if exceeded, the snapshot need to recreate
  private final int snapshotRetentionTimeInMin;
  // Max timeout for blob transfer in minutes in server side, to avoid endless sending files.
  private final int blobTransferMaxTimeoutInMin;
  // Max timeout for blob receive in minutes in client side, to avoid endless receiving files.
  private final int blobReceiveMaxTimeoutInMin;
  // Reader idle time in seconds in client side, to avoid the case the server shuts down before transfer completes.
  private final int blobReceiveReaderIdleTimeInSeconds;
  // Table format
  private final BlobTransferUtils.BlobTransferTableFormat transferSnapshotTableFormat;
  // Peers connectivity records freshness in seconds.
  private final int peersConnectivityFreshnessInSeconds;
  // Client read limit in bytes per second for all blob transfer channels
  private final long blobTransferClientReadLimitBytesPerSec;
  // Service write limit in bytes per second for all blob transfer channels
  private final long blobTransferServiceWriteLimitBytesPerSec;
  // Interval in mins for snapshot manager to clean up old snapshots
  private final int snapshotCleanupIntervalInMins;
  // Max concurrent replicas that is allowed to receive blob data simultaneously
  private final int maxConcurrentBlobReceiveReplicas;

  public P2PBlobTransferConfig(
      int p2pTransferServerPort,
      int p2pTransferClientPort,
      String baseDir,
      int maxConcurrentSnapshotUser,
      int snapshotRetentionTimeInMin,
      int blobTransferMaxTimeoutInMin,
      int blobReceiveMaxTimeoutInMin,
      int blobReceiveReaderIdleTimeInSeconds,
      BlobTransferUtils.BlobTransferTableFormat transferSnapshotTableFormat,
      int peersConnectivityFreshnessInSeconds,
      long blobTransferClientReadLimitBytesPerSec,
      long blobTransferServiceWriteLimitBytesPerSec,
      int snapshotCleanupIntervalInMins,
      int maxConcurrentBlobReceiveReplicas) {
    this.p2pTransferServerPort = p2pTransferServerPort;
    this.p2pTransferClientPort = p2pTransferClientPort;
    this.baseDir = baseDir;
    this.maxConcurrentSnapshotUser = maxConcurrentSnapshotUser;
    this.snapshotRetentionTimeInMin = snapshotRetentionTimeInMin;
    this.blobTransferMaxTimeoutInMin = blobTransferMaxTimeoutInMin;
    this.blobReceiveMaxTimeoutInMin = blobReceiveMaxTimeoutInMin;
    this.blobReceiveReaderIdleTimeInSeconds = blobReceiveReaderIdleTimeInSeconds;
    this.transferSnapshotTableFormat = transferSnapshotTableFormat;
    this.peersConnectivityFreshnessInSeconds = peersConnectivityFreshnessInSeconds;
    this.blobTransferClientReadLimitBytesPerSec = blobTransferClientReadLimitBytesPerSec;
    this.blobTransferServiceWriteLimitBytesPerSec = blobTransferServiceWriteLimitBytesPerSec;
    this.snapshotCleanupIntervalInMins = snapshotCleanupIntervalInMins;
    this.maxConcurrentBlobReceiveReplicas = maxConcurrentBlobReceiveReplicas;
  }

  public int getP2pTransferServerPort() {
    return p2pTransferServerPort;
  }

  public int getP2pTransferClientPort() {
    return p2pTransferClientPort;
  }

  public String getBaseDir() {
    return baseDir;
  }

  public int getMaxConcurrentSnapshotUser() {
    return maxConcurrentSnapshotUser;
  }

  public int getSnapshotRetentionTimeInMin() {
    return snapshotRetentionTimeInMin;
  }

  public int getBlobTransferMaxTimeoutInMin() {
    return blobTransferMaxTimeoutInMin;
  }

  public int getBlobReceiveTimeoutInMin() {
    return blobReceiveMaxTimeoutInMin;
  }

  public int getBlobReceiveReaderIdleTimeInSeconds() {
    return blobReceiveReaderIdleTimeInSeconds;
  }

  public BlobTransferUtils.BlobTransferTableFormat getTransferSnapshotTableFormat() {
    return transferSnapshotTableFormat;
  }

  public int getPeersConnectivityFreshnessInSeconds() {
    return peersConnectivityFreshnessInSeconds;
  }

  public long getBlobTransferClientReadLimitBytesPerSec() {
    return blobTransferClientReadLimitBytesPerSec;
  }

  public long getBlobTransferServiceWriteLimitBytesPerSec() {
    return blobTransferServiceWriteLimitBytesPerSec;
  }

  public int getSnapshotCleanupIntervalInMins() {
    return snapshotCleanupIntervalInMins;
  }

  public int getMaxConcurrentBlobReceiveReplicas() {
    return maxConcurrentBlobReceiveReplicas;
  }
}
