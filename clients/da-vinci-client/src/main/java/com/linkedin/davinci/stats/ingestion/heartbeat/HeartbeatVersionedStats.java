package com.linkedin.davinci.stats.ingestion.heartbeat;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.davinci.stats.AbstractVeniceAggVersionedStats;
import com.linkedin.davinci.stats.OtelVersionedStatsUtils;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.stats.StatsSupplier;
import com.linkedin.venice.stats.dimensions.ReplicaState;
import com.linkedin.venice.stats.dimensions.ReplicaType;
import com.linkedin.venice.stats.dimensions.VeniceChunkingStatus;
import com.linkedin.venice.stats.dimensions.VeniceRegionLocality;
import com.linkedin.venice.stats.dimensions.VeniceReplicationMode;
import com.linkedin.venice.stats.dimensions.VeniceStoreWriteType;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.function.Supplier;


/**
 * Manages Tehuti and OTel heartbeat/record-level delay stats per store.
 *
 * <p><b>OTel stats lifecycle:</b> OTel stats are created lazily on first metric recording via
 * {@link #getOrCreateHeartbeatOtelStats} and {@link #getOrCreateRecordLevelDelayOtelStats}.
 * Version info updates from {@link #handleStoreChanged} propagate to existing OTel stats
 * via {@link #onVersionInfoUpdated} ({@code computeIfPresent}). See
 * {@link #getOrCreateHeartbeatOtelStats} for why version info is fetched outside the lambda.
 */
public class HeartbeatVersionedStats extends AbstractVeniceAggVersionedStats<HeartbeatStat, HeartbeatStatReporter> {
  private final Map<HeartbeatKey, IngestionTimestampEntry> leaderMonitors;
  private final Map<HeartbeatKey, IngestionTimestampEntry> followerMonitors;

  private final PerStoreVersionedOtelStats<HeartbeatOtelStats> heartbeatOtelStats;
  private final PerStoreVersionedOtelStats<RecordLevelDelayOtelStats> recordLevelDelayOtelStats;
  private final String clusterName;

  // Time supplier for testability: defaults to System.currentTimeMillis()
  private Supplier<Long> currentTimeSupplier = System::currentTimeMillis;

  public HeartbeatVersionedStats(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      Supplier<HeartbeatStat> statsInitiator,
      StatsSupplier<HeartbeatStatReporter> reporterSupplier,
      Map<HeartbeatKey, IngestionTimestampEntry> leaderMonitors,
      Map<HeartbeatKey, IngestionTimestampEntry> followerMonitors,
      String clusterName) {
    super(metricsRepository, metadataRepository, statsInitiator, reporterSupplier, true);
    this.leaderMonitors = leaderMonitors;
    this.followerMonitors = followerMonitors;
    this.clusterName = clusterName;
    this.heartbeatOtelStats =
        createPerStoreOtelStats(storeName -> new HeartbeatOtelStats(getMetricsRepository(), storeName, clusterName));
    this.recordLevelDelayOtelStats = createPerStoreOtelStats(
        storeName -> new RecordLevelDelayOtelStats(getMetricsRepository(), storeName, clusterName));
  }

  public void recordLeaderLag(
      String storeName,
      int version,
      String region,
      long heartbeatTs,
      boolean isReadyToServe,
      VeniceStoreWriteType writeType,
      VeniceChunkingStatus chunkingStatus,
      VeniceRegionLocality locality,
      VeniceReplicationMode replicationMode) {
    // Calculate current time and delay once for both Tehuti and OTel metrics
    long currentTime = currentTimeSupplier.get();
    long delay = currentTime - heartbeatTs;

    // Tehuti metrics
    getStats(storeName, version).recordReadyToServeLeaderLag(region, delay, currentTime);

    // OTel metrics
    ReplicaState replicaState = isReadyToServe ? ReplicaState.READY_TO_SERVE : ReplicaState.CATCHING_UP;
    getOrCreateHeartbeatOtelStats(storeName).recordHeartbeatDelayOtelMetrics(
        version,
        region,
        ReplicaType.LEADER,
        replicaState,
        writeType,
        chunkingStatus,
        locality,
        replicationMode,
        delay);
  }

  public void recordFollowerLag(
      String storeName,
      int version,
      String region,
      long heartbeatTs,
      boolean isReadyToServe,
      VeniceStoreWriteType writeType,
      VeniceChunkingStatus chunkingStatus,
      VeniceRegionLocality locality,
      VeniceReplicationMode replicationMode) {
    // Calculate current time and delay once for all metrics
    long currentTime = currentTimeSupplier.get();
    long delay = currentTime - heartbeatTs;

    // If the partition is ready to serve, report it's lag to the main lag metric. Otherwise, report it
    // to the catch up metric.
    // The metric which isn't updated is squelched by reporting delay=0 (to appear caught up and mute alerts)
    long readyToServeDelay = isReadyToServe ? delay : 0;
    long catchingUpDelay = isReadyToServe ? 0 : delay;

    // Record to both Tehuti sensors (one gets actual delay, other gets 0 for squelching)
    getStats(storeName, version).recordReadyToServeFollowerLag(region, readyToServeDelay, currentTime);
    getStats(storeName, version).recordCatchingUpFollowerLag(region, catchingUpDelay, currentTime);

    // Record OTel only to the replica's actual state. Tehuti keeps the inactive-sensor squelch above.
    ReplicaState replicaState = isReadyToServe ? ReplicaState.READY_TO_SERVE : ReplicaState.CATCHING_UP;
    getOrCreateHeartbeatOtelStats(storeName).recordHeartbeatDelayOtelMetrics(
        version,
        region,
        ReplicaType.FOLLOWER,
        replicaState,
        writeType,
        chunkingStatus,
        locality,
        replicationMode,
        delay);
  }

  public void recordLeaderRecordLag(
      String storeName,
      int version,
      String region,
      long recordTs,
      boolean isReadyToServe,
      VeniceStoreWriteType writeType,
      VeniceChunkingStatus chunkingStatus,
      VeniceRegionLocality locality,
      VeniceReplicationMode replicationMode) {
    long currentTime = currentTimeSupplier.get();
    long delay = currentTime - recordTs;

    // OTel metrics only (no Tehuti for record-level delays)
    ReplicaState replicaState = isReadyToServe ? ReplicaState.READY_TO_SERVE : ReplicaState.CATCHING_UP;
    getOrCreateRecordLevelDelayOtelStats(storeName).recordRecordDelayOtelMetrics(
        version,
        region,
        ReplicaType.LEADER,
        replicaState,
        writeType,
        chunkingStatus,
        locality,
        replicationMode,
        delay);
  }

  public void recordFollowerRecordLag(
      String storeName,
      int version,
      String region,
      long recordTs,
      boolean isReadyToServe,
      VeniceStoreWriteType writeType,
      VeniceChunkingStatus chunkingStatus,
      VeniceRegionLocality locality,
      VeniceReplicationMode replicationMode) {
    long currentTime = currentTimeSupplier.get();
    long delay = currentTime - recordTs;

    // OTel metrics only (no Tehuti for record-level delays)
    ReplicaState replicaState = isReadyToServe ? ReplicaState.READY_TO_SERVE : ReplicaState.CATCHING_UP;
    getOrCreateRecordLevelDelayOtelStats(storeName).recordRecordDelayOtelMetrics(
        version,
        region,
        ReplicaType.FOLLOWER,
        replicaState,
        writeType,
        chunkingStatus,
        locality,
        replicationMode,
        delay);
  }

  /** No-op: heartbeat stats are loaded lazily when the first heartbeat/record arrives. */
  @Override
  public synchronized void loadAllStats() {
    // No-op
  }

  @Override
  public void handleStoreCreated(Store store) {
    // No-op
  }

  @Override
  public void handleStoreChanged(Store store) {
    String storeName = store.getName();
    if (isStoreAssignedToThisNode(storeName)) {
      updateStatsVersionInfo(storeName, store.getVersions(), store.getCurrentVersion());
    } else {
      // Tehuti skips stores with no replica here; OTel still follows them so a returning replica gets the right role.
      onVersionInfoUpdated(
          storeName,
          store.getCurrentVersion(),
          OtelVersionedStatsUtils.computeFutureVersion(store.getVersions()));
    }
  }

  boolean isStoreAssignedToThisNode(String store) {
    if (leaderMonitors == null || followerMonitors == null) {
      // TODO: We have to do this because theres a self call in the constructor
      // of the superclass of this class. We shouldn't have to do this
      return false;
    }
    for (HeartbeatKey key: leaderMonitors.keySet()) {
      if (key.storeName.equals(store)) {
        return true;
      }
    }
    for (HeartbeatKey key: followerMonitors.keySet()) {
      if (key.storeName.equals(store)) {
        return true;
      }
    }
    return false;
  }

  private HeartbeatOtelStats getOrCreateHeartbeatOtelStats(String storeName) {
    return heartbeatOtelStats.getOrCreate(storeName);
  }

  private RecordLevelDelayOtelStats getOrCreateRecordLevelDelayOtelStats(String storeName) {
    return recordLevelDelayOtelStats.getOrCreate(storeName);
  }

  /**
   * Emits a per-record OTel metric for leader record delay (called per record, not aggregated).
   * Uses {@code get()} as a fast path; falls back to {@code getOrCreate} on first call per store.
   */
  public void emitPerRecordLeaderOtelMetric(
      String storeName,
      int version,
      String region,
      long delay,
      boolean isReadyToServe,
      VeniceStoreWriteType writeType,
      VeniceChunkingStatus chunkingStatus,
      VeniceRegionLocality locality,
      VeniceReplicationMode replicationMode) {
    RecordLevelDelayOtelStats otelStats = getOrLazilyCreateRecordLevelDelayOtelStats(storeName);
    if (otelStats == null || !otelStats.emitOtelMetrics()) {
      return;
    }
    ReplicaState replicaState = isReadyToServe ? ReplicaState.READY_TO_SERVE : ReplicaState.CATCHING_UP;
    otelStats.recordRecordDelayOtelMetrics(
        version,
        region,
        ReplicaType.LEADER,
        replicaState,
        writeType,
        chunkingStatus,
        locality,
        replicationMode,
        delay);
  }

  /**
   * Emits a per-record OTel metric for follower record delay (called per record, not aggregated).
   * Uses {@code get()} as a fast path; falls back to {@code getOrCreate} on first call per store.
   */
  public void emitPerRecordFollowerOtelMetric(
      String storeName,
      int version,
      String region,
      long delay,
      boolean isReadyToServe,
      VeniceStoreWriteType writeType,
      VeniceChunkingStatus chunkingStatus,
      VeniceRegionLocality locality,
      VeniceReplicationMode replicationMode) {
    RecordLevelDelayOtelStats otelStats = getOrLazilyCreateRecordLevelDelayOtelStats(storeName);
    if (otelStats == null || !otelStats.emitOtelMetrics()) {
      return;
    }
    ReplicaState replicaState = isReadyToServe ? ReplicaState.READY_TO_SERVE : ReplicaState.CATCHING_UP;
    otelStats.recordRecordDelayOtelMetrics(
        version,
        region,
        ReplicaType.FOLLOWER,
        replicaState,
        writeType,
        chunkingStatus,
        locality,
        replicationMode,
        delay);
  }

  /**
   * Fast-path lookup with lazy initialization fallback.
   * Returns null if the store is not found in the metadata repository (e.g., store was deleted).
   */
  private RecordLevelDelayOtelStats getOrLazilyCreateRecordLevelDelayOtelStats(String storeName) {
    RecordLevelDelayOtelStats existing = recordLevelDelayOtelStats.get(storeName);
    if (existing != null) {
      return existing;
    }
    if (!metadataRepository.hasStore(storeName)) {
      return null;
    }
    return getOrCreateRecordLevelDelayOtelStats(storeName);
  }

  @VisibleForTesting
  HeartbeatStat getStatsForTesting(String storeName, int version) {
    return getStats(storeName, version);
  }

  @VisibleForTesting
  HeartbeatOtelStats getOtelStatsForTesting(String storeName) {
    return heartbeatOtelStats.get(storeName);
  }

  @VisibleForTesting
  RecordLevelDelayOtelStats getRecordLevelDelayOtelStatsForTesting(String storeName) {
    return recordLevelDelayOtelStats.get(storeName);
  }

  @VisibleForTesting
  void setCurrentTimeSupplier(Supplier<Long> timeSupplier) {
    this.currentTimeSupplier = timeSupplier;
  }
}
