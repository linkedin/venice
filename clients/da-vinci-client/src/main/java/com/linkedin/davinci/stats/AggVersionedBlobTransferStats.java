package com.linkedin.davinci.stats;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;


/**
 * Aggregates blob transfer statistics at the store version level.
 * This class manages versioned statistics for blob transfer operations, tracking metrics such as
 * response counts, throughput, transfer times, and bytes sent/received for each store version.
 * It extends {@link AbstractVeniceAggVersionedStats} to provide automatic aggregation across
 * all versions of a store.
 *
 * <p><b>OTel stats lifecycle:</b> OTel stats are created lazily by {@link #getBlobTransferOtelStats}
 * and updated by {@link #onVersionInfoUpdated} via {@code computeIfPresent}. This class uses
 * eager loading ({@code loadAllStats()} is NOT overridden), so {@code onVersionInfoUpdated}
 * needs a null guard because it is called during the super() constructor before subclass fields
 * ({@code otelStatsMap}) are initialized. {@code cleanupVersionResources} is not overridden
 * because {@link BlobTransferOtelStats} has no per-version state to clean up — version
 * classification is handled by volatile VersionInfo, which is updated separately.
 */
public class AggVersionedBlobTransferStats
    extends AbstractVeniceAggVersionedStats<BlobTransferStats, BlobTransferStatsReporter> {
  /**
   * Lazy per-store OTel stats. Bounded by the number of active stores on this server (typically
   * tens, not hundreds). Entries are created on first recording via {@link #getBlobTransferOtelStats}
   * and removed in {@link #handleStoreDeleted}. No periodic eviction — cleanup is tied to store
   * lifecycle events.
   */
  private final Map<String, BlobTransferOtelStats> otelStatsMap = new VeniceConcurrentHashMap<>();
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
  }

  /** Updates version info for existing OTel stats only. Null guard needed because eager loading
   *  calls this from the super() constructor before {@code otelStatsMap} is initialized. */
  @Override
  protected void onVersionInfoUpdated(String storeName, int currentVersion, int futureVersion) {
    if (otelStatsMap == null) {
      return; // Called during super() constructor before otelStatsMap is initialized
    }
    otelStatsMap.computeIfPresent(storeName, (k, stats) -> {
      stats.updateVersionInfo(currentVersion, futureVersion);
      return stats;
    });
  }

  @Override
  public void handleStoreDeleted(String storeName) {
    try {
      super.handleStoreDeleted(storeName);
    } finally {
      otelStatsMap.remove(storeName);
    }
  }

  /**
   * Gets or creates OTel stats for a store. {@code getCurrentVersion}/{@code getFutureVersion}
   * are called <b>before</b> {@code computeIfAbsent} because they can trigger
   * {@code addStore} → {@code onVersionInfoUpdated} → {@code otelStatsMap.computeIfPresent},
   * which would re-enter this same map from inside the lambda (violates ConcurrentHashMap contract).
   * The {@code get()} fast-path skips these calls when stats already exist.
   */
  private BlobTransferOtelStats getBlobTransferOtelStats(String storeName) {
    BlobTransferOtelStats existing = otelStatsMap.get(storeName);
    if (existing != null) {
      return existing;
    }
    int currentVersion = getCurrentVersion(storeName);
    int futureVersion = getFutureVersion(storeName);
    return otelStatsMap.computeIfAbsent(storeName, k -> {
      BlobTransferOtelStats stats = new BlobTransferOtelStats(getMetricsRepository(), k, clusterName);
      stats.updateVersionInfo(currentVersion, futureVersion);
      return stats;
    });
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
