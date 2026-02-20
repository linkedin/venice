package com.linkedin.davinci.stats.ingestion.heartbeat;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.davinci.stats.AbstractVeniceAggVersionedStats;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.stats.StatsSupplier;
import com.linkedin.venice.stats.dimensions.ReplicaState;
import com.linkedin.venice.stats.dimensions.ReplicaType;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.function.Supplier;


public class HeartbeatVersionedStats extends AbstractVeniceAggVersionedStats<HeartbeatStat, HeartbeatStatReporter> {
  private final Map<HeartbeatKey, IngestionTimestampEntry> leaderMonitors;
  private final Map<HeartbeatKey, IngestionTimestampEntry> followerMonitors;

  // OpenTelemetry metrics per store
  private final Map<String, HeartbeatOtelStats> heartbeatOtelStatsMap;
  private final Map<String, RecordLevelDelayOtelStats> recordLevelDelayOtelStatsMap;
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
    this.heartbeatOtelStatsMap = new VeniceConcurrentHashMap<>();
    this.recordLevelDelayOtelStatsMap = new VeniceConcurrentHashMap<>();
  }

  public void recordLeaderLag(String storeName, int version, String region, long heartbeatTs) {
    // Calculate current time and delay once for both Tehuti and OTel metrics
    long currentTime = currentTimeSupplier.get();
    long delay = currentTime - heartbeatTs;

    // Tehuti metrics
    getStats(storeName, version).recordReadyToServeLeaderLag(region, delay, currentTime);

    // OTel metrics
    getOrCreateHeartbeatOtelStats(storeName).recordHeartbeatDelayOtelMetrics(
        version,
        region,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE, // Leaders are always ready to serve
        delay);
  }

  public void recordFollowerLag(
      String storeName,
      int version,
      String region,
      long heartbeatTs,
      boolean isReadyToServe) {
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

    // Record to both OTel dimensions (one gets actual delay, other gets 0 for squelching)
    HeartbeatOtelStats otelStats = getOrCreateHeartbeatOtelStats(storeName);
    otelStats.recordHeartbeatDelayOtelMetrics(
        version,
        region,
        ReplicaType.FOLLOWER,
        ReplicaState.READY_TO_SERVE,
        readyToServeDelay);
    otelStats.recordHeartbeatDelayOtelMetrics(
        version,
        region,
        ReplicaType.FOLLOWER,
        ReplicaState.CATCHING_UP,
        catchingUpDelay);
  }

  public void recordLeaderRecordLag(String storeName, int version, String region, long recordTs) {
    long currentTime = currentTimeSupplier.get();
    long delay = currentTime - recordTs;

    // OTel metrics only (no Tehuti for record-level delays)
    getOrCreateRecordLevelDelayOtelStats(storeName).recordRecordDelayOtelMetrics(
        version,
        region,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE, // Leaders are always ready to serve
        delay);
  }

  public void recordFollowerRecordLag(
      String storeName,
      int version,
      String region,
      long recordTs,
      boolean isReadyToServe) {
    long currentTime = currentTimeSupplier.get();
    long delay = currentTime - recordTs;

    long readyToServeDelay = isReadyToServe ? delay : 0;
    long catchingUpDelay = isReadyToServe ? 0 : delay;

    // OTel metrics only (no Tehuti for record-level delays)
    RecordLevelDelayOtelStats otelStats = getOrCreateRecordLevelDelayOtelStats(storeName);
    otelStats.recordRecordDelayOtelMetrics(
        version,
        region,
        ReplicaType.FOLLOWER,
        ReplicaState.READY_TO_SERVE,
        readyToServeDelay);
    otelStats
        .recordRecordDelayOtelMetrics(version, region, ReplicaType.FOLLOWER, ReplicaState.CATCHING_UP, catchingUpDelay);
  }

  @Override
  public synchronized void loadAllStats() {
    // No-op
  }

  @Override
  public void handleStoreCreated(Store store) {
    // No-op
  }

  @Override
  public void handleStoreDeleted(String storeName) {
    try {
      super.handleStoreDeleted(storeName);
    } finally {
      HeartbeatOtelStats otelStats = heartbeatOtelStatsMap.remove(storeName);
      if (otelStats != null) {
        otelStats.close();
      }
      RecordLevelDelayOtelStats recordStats = recordLevelDelayOtelStatsMap.remove(storeName);
      if (recordStats != null) {
        recordStats.close();
      }
    }
  }

  @Override
  public void handleStoreChanged(Store store) {
    if (isStoreAssignedToThisNode(store.getName())) {
      updateStatsVersionInfo(store.getName(), store.getVersions(), store.getCurrentVersion());
    }
  }

  @Override
  protected void onVersionInfoUpdated(String storeName, int currentVersion, int futureVersion) {
    if (heartbeatOtelStatsMap == null || recordLevelDelayOtelStatsMap == null) {
      return;
    }
    heartbeatOtelStatsMap.computeIfPresent(storeName, (store, stats) -> {
      stats.updateVersionInfo(currentVersion, futureVersion);
      return stats;
    });
    recordLevelDelayOtelStatsMap.computeIfPresent(storeName, (store, stats) -> {
      stats.updateVersionInfo(currentVersion, futureVersion);
      return stats;
    });
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

  /**
   * Gets or creates OTel stats for a store. Calls getCurrentVersion/getFutureVersion outside the
   * computeIfAbsent lambda to avoid recursive ConcurrentHashMap updates: getCurrentVersion can
   * trigger store registration -> onVersionInfoUpdated -> computeIfPresent on the same map/key.
   */
  private HeartbeatOtelStats getOrCreateHeartbeatOtelStats(String storeName) {
    HeartbeatOtelStats existing = heartbeatOtelStatsMap.get(storeName);
    if (existing != null) {
      return existing;
    }
    int currentVersion = getCurrentVersion(storeName);
    int futureVersion = getFutureVersion(storeName);
    return heartbeatOtelStatsMap.computeIfAbsent(storeName, key -> {
      HeartbeatOtelStats stats = new HeartbeatOtelStats(getMetricsRepository(), storeName, clusterName);
      stats.updateVersionInfo(currentVersion, futureVersion);
      return stats;
    });
  }

  /** @see #getOrCreateHeartbeatOtelStats for pattern explanation */
  private RecordLevelDelayOtelStats getOrCreateRecordLevelDelayOtelStats(String storeName) {
    RecordLevelDelayOtelStats existing = recordLevelDelayOtelStatsMap.get(storeName);
    if (existing != null) {
      return existing;
    }
    int currentVersion = getCurrentVersion(storeName);
    int futureVersion = getFutureVersion(storeName);
    return recordLevelDelayOtelStatsMap.computeIfAbsent(storeName, key -> {
      RecordLevelDelayOtelStats stats = new RecordLevelDelayOtelStats(getMetricsRepository(), storeName, clusterName);
      stats.updateVersionInfo(currentVersion, futureVersion);
      return stats;
    });
  }

  /**
   * Emits a per-record OTel metric for leader record delay immediately (not aggregated).
   * This is called for every record when per-record OTel metrics are enabled.
   * Uses map.get() instead of computeIfAbsent to avoid synchronization overhead on hot path.
   * Returns early if stats not initialized (store not registered yet).
   *
   * @param storeName The name of the store
   * @param version The version number
   * @param region The region name
   * @param delay The delay in milliseconds since record was produced
   */
  public void emitPerRecordLeaderOtelMetric(String storeName, int version, String region, long delay) {
    RecordLevelDelayOtelStats otelStats = recordLevelDelayOtelStatsMap.get(storeName);
    if (otelStats == null || !otelStats.emitOtelMetrics()) {
      return; // Fast path exit: stats not initialized or OTel disabled
    }
    otelStats.recordRecordDelayOtelMetrics(version, region, ReplicaType.LEADER, ReplicaState.READY_TO_SERVE, delay);
  }

  /**
   * Emits a per-record OTel metric for follower record delay immediately (not aggregated).
   * This is called for every record when per-record OTel metrics are enabled.
   * Uses map.get() instead of computeIfAbsent to avoid synchronization overhead on hot path.
   * Returns early if stats not initialized (store not registered yet).
   *
   * @param storeName The name of the store
   * @param version The version number
   * @param region The region name
   * @param delay The delay in milliseconds since record was produced
   * @param isReadyToServe Whether the partition is ready to serve
   */
  public void emitPerRecordFollowerOtelMetric(
      String storeName,
      int version,
      String region,
      long delay,
      boolean isReadyToServe) {
    RecordLevelDelayOtelStats otelStats = recordLevelDelayOtelStatsMap.get(storeName);
    if (otelStats == null || !otelStats.emitOtelMetrics()) {
      return; // Fast path exit: stats not initialized or OTel disabled
    }
    ReplicaState replicaState = isReadyToServe ? ReplicaState.READY_TO_SERVE : ReplicaState.CATCHING_UP;
    otelStats.recordRecordDelayOtelMetrics(version, region, ReplicaType.FOLLOWER, replicaState, delay);
  }

  @VisibleForTesting
  HeartbeatStat getStatsForTesting(String storeName, int version) {
    return getStats(storeName, version);
  }

  @VisibleForTesting
  HeartbeatOtelStats getOtelStatsForTesting(String storeName) {
    return heartbeatOtelStatsMap.get(storeName);
  }

  @VisibleForTesting
  void setCurrentTimeSupplier(Supplier<Long> timeSupplier) {
    this.currentTimeSupplier = timeSupplier;
  }
}
