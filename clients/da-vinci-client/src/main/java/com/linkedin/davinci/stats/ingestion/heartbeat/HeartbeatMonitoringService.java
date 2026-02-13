package com.linkedin.davinci.stats.ingestion.heartbeat;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.davinci.kafka.consumer.ReplicaHeartbeatInfo;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.davinci.stats.HeartbeatMonitoringServiceStats;
import com.linkedin.venice.exceptions.VeniceNoHelixResourceException;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreVersionInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This service monitors heartbeats.  Heartbeats are only monitored if lagMonitors are added for leader or follower
 * partitions.  Once a lagMonitor is added, the service will being emitting a metric which grows linearly with time,
 * only resetting to the timestamp of the last reported heartbeat for a given partition.
 *
 * Heartbeats are only monitored for stores which have a hybrid config. All other registrations for lag monitoring
 * are ignored.
 *
 * Max and Average are reported per version of resource across partitions.
 *
 * If a heartbeat is invoked for a partition that we're NOT monitoring lag for, it is ignored.
 *
 * This class will monitor lag for a partition as a leader or follower, but never both.  Whether we're reporting
 * leader or follower depends on which monitor was set last.
 *
 * Lag will stop being reported for partitions which have the monitor removed.
 *
 * Each region gets a different lag monitor
 */
public class HeartbeatMonitoringService extends AbstractVeniceService {
  public static final int DEFAULT_REPORTER_THREAD_SLEEP_INTERVAL_SECONDS = 60;
  public static final int DEFAULT_LAG_LOGGING_THREAD_SLEEP_INTERVAL_SECONDS = 60;
  public static final long DEFAULT_STALE_HEARTBEAT_LOG_THRESHOLD_MILLIS = TimeUnit.MINUTES.toMillis(10);
  public static final long INVALID_MESSAGE_TIMESTAMP = -1;
  public static final long INVALID_HEARTBEAT_LAG = Long.MAX_VALUE;
  public static final int DEFAULT_LAG_MONITOR_CLEANUP_CYCLE = 5;
  private static final Logger LOGGER = LogManager.getLogger(HeartbeatMonitoringService.class);
  private final ReadOnlyStoreRepository metadataRepository;
  private final Thread reportingThread;
  private final Thread lagCleanupAndLoggingThread;
  private final VeniceServerConfig serverConfig;
  private final Set<String> regionNames;
  private final String localRegionName;
  private final AtomicBoolean heartbeatLagCleanupAndLoggingThreadIsRunning = new AtomicBoolean(false);
  private final AtomicBoolean heartbeatReporterThreadIsRunning = new AtomicBoolean(false);

  // Flat maps keyed by (storeName, version, partition, region) for concurrent access without intermediate contention
  private final Map<HeartbeatKey, IngestionTimestampEntry> followerHeartbeatTimeStamps;
  private final Map<HeartbeatKey, IngestionTimestampEntry> leaderHeartbeatTimeStamps;
  private final Map<String, Integer> cleanupHeartbeatMap;
  private final HeartbeatVersionedStats versionStatsReporter;
  private final HeartbeatMonitoringServiceStats heartbeatMonitoringServiceStats;
  private final Duration maxWaitForVersionInfo;
  private final CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewRepositoryFuture;
  private final String nodeId;
  private final int lagMonitorCleanupCycle;
  private final boolean recordLevelTimestampEnabled;
  private final boolean perRecordOtelMetricsEnabled;
  private HelixCustomizedViewOfflinePushRepository customizedViewRepository;
  private KafkaStoreIngestionService kafkaStoreIngestionService;

  public HeartbeatMonitoringService(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      VeniceServerConfig serverConfig,
      HeartbeatMonitoringServiceStats heartbeatMonitoringServiceStats,
      CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewRepositoryFuture) {
    this.regionNames = serverConfig.getRegionNames();
    this.localRegionName = serverConfig.getRegionName();
    this.maxWaitForVersionInfo = serverConfig.getServerMaxWaitForVersionInfo();
    this.reportingThread = new HeartbeatReporterThread(serverConfig);
    this.lagCleanupAndLoggingThread = new HeartbeatLagCleanupAndLoggingThread(serverConfig);
    this.followerHeartbeatTimeStamps = new VeniceConcurrentHashMap<>();
    this.leaderHeartbeatTimeStamps = new VeniceConcurrentHashMap<>();
    this.cleanupHeartbeatMap = new VeniceConcurrentHashMap<>();
    this.metadataRepository = metadataRepository;
    this.versionStatsReporter = new HeartbeatVersionedStats(
        metricsRepository,
        metadataRepository,
        () -> new HeartbeatStat(new MetricConfig(), regionNames),
        (aMetricsRepository, storeName, clusterName) -> new HeartbeatStatReporter(
            aMetricsRepository,
            storeName,
            regionNames),
        leaderHeartbeatTimeStamps,
        followerHeartbeatTimeStamps,
        serverConfig.getClusterName());
    this.heartbeatMonitoringServiceStats = heartbeatMonitoringServiceStats;
    this.customizedViewRepositoryFuture = customizedViewRepositoryFuture;
    this.nodeId = Utils.getHelixNodeIdentifier(serverConfig.getListenerHostname(), serverConfig.getListenerPort());
    this.lagMonitorCleanupCycle = serverConfig.getLagMonitorCleanupCycle();
    this.recordLevelTimestampEnabled = serverConfig.isRecordLevelTimestampEnabled();
    this.perRecordOtelMetricsEnabled = serverConfig.isPerRecordOtelMetricsEnabled();
    this.serverConfig = serverConfig;
  }

  private synchronized void initializeEntry(
      Map<HeartbeatKey, IngestionTimestampEntry> heartbeatTimestamps,
      Version version,
      int partition,
      boolean isFollower) {
    // We don't monitor heartbeat lag for non-hybrid versions
    if (version.getHybridStoreConfig() == null) {
      return;
    }
    String storeName = version.getStoreName();
    int versionNum = version.getNumber();
    long currentTime = System.currentTimeMillis();
    if (version.isActiveActiveReplicationEnabled()) {
      for (String region: regionNames) {
        if (Utils.isSeparateTopicRegion(region) && !version.isSeparateRealTimeTopicEnabled()) {
          continue;
        }
        HeartbeatKey key = new HeartbeatKey(storeName, versionNum, partition, region);
        heartbeatTimestamps.putIfAbsent(key, new IngestionTimestampEntry(currentTime, currentTime, false, false));
      }
    } else {
      HeartbeatKey key = new HeartbeatKey(storeName, versionNum, partition, localRegionName);
      heartbeatTimestamps.putIfAbsent(key, new IngestionTimestampEntry(currentTime, currentTime, false, false));
    }
    LOGGER.info(
        "Completed initializing heartbeat timestamps for version: {}, partition: {}, follower: {}",
        version,
        partition,
        isFollower);
  }

  private synchronized void removeEntry(
      Map<HeartbeatKey, IngestionTimestampEntry> heartbeatTimestamps,
      Version version,
      int partition) {
    String storeName = version.getStoreName();
    int versionNum = version.getNumber();
    heartbeatTimestamps.keySet()
        .removeIf(key -> key.storeName.equals(storeName) && key.version == versionNum && key.partition == partition);
  }

  /**
   * Adds monitoring for a follower partition of a given version. This request is ignored if the version
   * isn't hybrid.
   *
   * @param version the version to monitor lag for
   * @param partition the partition to monitor lag for
   */
  public void addFollowerLagMonitor(Version version, int partition) {
    String cleanupKey = Utils.getReplicaId(version.kafkaTopicName(), partition);
    cleanupHeartbeatMap.compute(cleanupKey, (k, v) -> {
      // See comments in {@link #addLeaderLagMonitor(Version, int)} for race condition explanations
      initializeEntry(followerHeartbeatTimeStamps, version, partition, true);
      removeEntry(leaderHeartbeatTimeStamps, version, partition);
      return null;
    });
  }

  /**
   * Adds monitoring for a leader partition of a given version. This request is ignored if the version
   * isn't hybrid.
   *
   * @param version the version to monitor lag for
   * @param partition the partition to monitor lag for
   */
  public void addLeaderLagMonitor(Version version, int partition) {
    String cleanupKey = Utils.getReplicaId(version.kafkaTopicName(), partition);
    cleanupHeartbeatMap.compute(cleanupKey, (k, v) -> {
      // Cleanup logic should perform the check and cleanup in a similar compute block to ensure that the following
      // race won't occur:
      // 1. A replica is deemed lingering and to be removed
      // 2. The same replica is reassigned to this node and added to lag monitor before the cleanup thread is able to
      // remove it.
      // 3. The cleanup thread removes the newly added lag monitor and the replica will be ingested without lag monitor
      initializeEntry(leaderHeartbeatTimeStamps, version, partition, false);
      removeEntry(followerHeartbeatTimeStamps, version, partition);
      return null;
    });
  }

  /**
   * Removes monitoring for a partition of a given version.
   *
   * @param version the version to remove monitoring for
   * @param partition the partition to remove monitoring for
   */
  public void removeLagMonitor(Version version, int partition) {
    removeEntry(leaderHeartbeatTimeStamps, version, partition);
    removeEntry(followerHeartbeatTimeStamps, version, partition);
  }

  public Map<String, ReplicaHeartbeatInfo> getHeartbeatInfo(
      String versionTopicName,
      int partitionFilter,
      boolean filterLagReplica) {
    Map<String, ReplicaHeartbeatInfo> aggregateResult = new VeniceConcurrentHashMap<>();
    long currentTimestamp = System.currentTimeMillis();
    aggregateResult.putAll(
        getHeartbeatInfoFromMap(
            leaderHeartbeatTimeStamps,
            LeaderFollowerStateType.LEADER.name(),
            currentTimestamp,
            versionTopicName,
            partitionFilter,
            filterLagReplica));
    aggregateResult.putAll(
        getHeartbeatInfoFromMap(
            followerHeartbeatTimeStamps,
            LeaderFollowerStateType.STANDBY.name(),
            currentTimestamp,
            versionTopicName,
            partitionFilter,
            filterLagReplica));
    return aggregateResult;
  }

  Map<String, ReplicaHeartbeatInfo> getHeartbeatInfoFromMap(
      Map<HeartbeatKey, IngestionTimestampEntry> heartbeatTimestampMap,
      String leaderState,
      long currentTimestamp,
      String versionTopicName,
      int partitionFilter,
      boolean filterLagReplica) {
    Map<String, ReplicaHeartbeatInfo> result = new VeniceConcurrentHashMap<>();
    String storeName = Version.parseStoreFromKafkaTopicName(versionTopicName);
    int version = Version.parseVersionFromKafkaTopicName(versionTopicName);

    if (partitionFilter >= 0) {
      // Targeted lookup: directly look up keys for the specific partition and each region
      for (String region: getRegionNames()) {
        HeartbeatKey key = new HeartbeatKey(storeName, version, partitionFilter, region);
        IngestionTimestampEntry entry = heartbeatTimestampMap.get(key);
        if (entry == null) {
          continue;
        }
        addHeartbeatInfoToResult(result, key, entry, leaderState, currentTimestamp, filterLagReplica);
      }
    } else {
      // No partition filter: use PCS from ingestion service to do targeted lookups per partition and region
      KafkaStoreIngestionService ingestionService = getKafkaStoreIngestionService();
      StoreIngestionTask sit =
          ingestionService == null ? null : ingestionService.getStoreIngestionTask(versionTopicName);
      if (sit != null) {
        for (PartitionConsumptionState pcs: sit.getPartitionConsumptionStates()) {
          for (String region: getRegionNames()) {
            HeartbeatKey key = pcs.getOrCreateCachedHeartbeatKey(region);
            IngestionTimestampEntry entry = heartbeatTimestampMap.get(key);
            if (entry == null) {
              continue;
            }
            addHeartbeatInfoToResult(result, key, entry, leaderState, currentTimestamp, filterLagReplica);
          }
        }
      }
    }
    return result;
  }

  private void addHeartbeatInfoToResult(
      Map<String, ReplicaHeartbeatInfo> result,
      HeartbeatKey key,
      IngestionTimestampEntry entry,
      String leaderState,
      long currentTimestamp,
      boolean filterLagReplica) {
    long heartbeatTs = entry.heartbeatTimestamp;
    long lag = currentTimestamp - heartbeatTs;
    if (filterLagReplica && lag < DEFAULT_STALE_HEARTBEAT_LOG_THRESHOLD_MILLIS) {
      return;
    }
    String replicaId = key.getReplicaId();
    ReplicaHeartbeatInfo replicaHeartbeatInfo =
        new ReplicaHeartbeatInfo(replicaId, key.region, leaderState, entry.readyToServe, heartbeatTs, lag);
    result.put(replicaId + "-" + key.region, replicaHeartbeatInfo);
  }

  @Override
  public boolean startInner() throws Exception {
    heartbeatLagCleanupAndLoggingThreadIsRunning.set(true);
    heartbeatReporterThreadIsRunning.set(true);
    reportingThread.start();
    lagCleanupAndLoggingThread.start();
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    heartbeatLagCleanupAndLoggingThreadIsRunning.set(false);
    heartbeatReporterThreadIsRunning.set(false);
    reportingThread.interrupt();
    lagCleanupAndLoggingThread.interrupt();
  }

  /**
   * Update lag monitor for a given resource replica based on different heartbeat lag monitor action.
   */
  public void updateLagMonitor(
      String resourceName,
      int partitionId,
      HeartbeatLagMonitorAction heartbeatLagMonitorAction) {
    try {
      String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
      int storeVersion = Version.parseVersionFromKafkaTopicName(resourceName);
      StoreVersionInfo res =
          getMetadataRepository().waitVersion(storeName, storeVersion, getMaxWaitForVersionInfo(), 200);
      Store store = res.getStore();
      Version version = res.getVersion();
      if (store == null) {
        LOGGER.error(
            "Failed to get store for resource: {} with trigger: {}. Will not update lag monitor.",
            Utils.getReplicaId(resourceName, partitionId),
            heartbeatLagMonitorAction.getTrigger());
        return;
      }
      if (version == null) {
        if (!HeartbeatLagMonitorAction.REMOVE_MONITOR.equals(heartbeatLagMonitorAction)) {
          LOGGER.error(
              "Failed to get version for resource: {} with trigger: {}. Will not update lag monitor.",
              Utils.getReplicaId(resourceName, partitionId),
              heartbeatLagMonitorAction.getTrigger());
          return;
        }
        // During version deletion, the version will be deleted from ZK prior to servers perform resource deletion.
        // It's valid to have null version when trying to remove lag monitor for the deleted resource.
        version = new VersionImpl(storeName, storeVersion, "");
      }
      switch (heartbeatLagMonitorAction) {
        case SET_LEADER_MONITOR:
          addLeaderLagMonitor(version, partitionId);
          break;
        case SET_FOLLOWER_MONITOR:
          addFollowerLagMonitor(version, partitionId);
          break;
        case REMOVE_MONITOR:
          removeLagMonitor(version, partitionId);
          break;
        default:
      }
    } catch (Exception e) {
      LOGGER.error(
          "Failed to update lag monitor for replica: {} with trigger: {}",
          Utils.getReplicaId(resourceName, partitionId),
          heartbeatLagMonitorAction.getTrigger(),
          e);
    }
  }

  /**
   * Get maximum heartbeat lag from all regions (except separate RT regions) for a given LEADER replica.
   * @return Max leader heartbeat lag, or {@link #INVALID_HEARTBEAT_LAG} if any region's heartbeat is unknown.
   */
  public long getReplicaLeaderMaxHeartbeatLag(
      PartitionConsumptionState partitionConsumptionState,
      boolean shouldLogLag) {
    return getReplicaLeaderMaxHeartbeatLag(partitionConsumptionState, shouldLogLag, System.currentTimeMillis());
  }

  /**
   * Get minimum heartbeat timestamp from all regions (except separate RT regions) for a given LEADER replica.
   * @return Min leader heartbeat timestamp, or {@link #INVALID_MESSAGE_TIMESTAMP} if any region's heartbeat is unknown.
   */
  public long getReplicaLeaderMinHeartbeatTimestamp(
      PartitionConsumptionState partitionConsumptionState,
      boolean shouldLogLag) {
    // Use a fixed timestamp so there is no value overflow.
    long currentTimestamp = System.currentTimeMillis();
    long lag = getReplicaLeaderMaxHeartbeatLag(partitionConsumptionState, shouldLogLag, currentTimestamp);
    if (lag == Long.MAX_VALUE) {
      return INVALID_MESSAGE_TIMESTAMP;
    } else {
      return currentTimestamp - lag;
    }
  }

  /**
   * Get heartbeat lag from local region for a given FOLLOWER replica.
   * @return Follower heartbeat lag, or {@link #INVALID_HEARTBEAT_LAG} if local region's heartbeat is unknown.
   */
  public long getReplicaFollowerHeartbeatLag(
      PartitionConsumptionState partitionConsumptionState,
      boolean shouldLogLag) {
    return getReplicaFollowerHeartbeatLag(partitionConsumptionState, shouldLogLag, System.currentTimeMillis());
  }

  /**
   * Get heartbeat timestamp from local region for a given FOLLOWER replica.
   * @return Follower heartbeat timestamp, or {@link #INVALID_MESSAGE_TIMESTAMP} if local region's heartbeat is unknown.
   */
  public long getReplicaFollowerHeartbeatTimestamp(
      PartitionConsumptionState partitionConsumptionState,
      boolean shouldLogLag) {
    long currentTimestamp = System.currentTimeMillis();
    long lag = getReplicaFollowerHeartbeatLag(partitionConsumptionState, shouldLogLag, currentTimestamp);
    if (lag == INVALID_HEARTBEAT_LAG) {
      return INVALID_MESSAGE_TIMESTAMP;
    }
    return currentTimestamp - lag;
  }

  long getReplicaLeaderMaxHeartbeatLag(
      PartitionConsumptionState partitionConsumptionState,
      boolean shouldLogLag,
      long currentTimestamp) {
    long maxLag = 0;
    boolean found = false;
    /**
     * When initializing A/A leader lag entry, we will initialize towards all available regions, so scanning this map
     * should be able to tell us all the lag information.
     * Use direct key lookups per region instead of iterating the entire map.
     */
    for (String region: getRegionNames()) {
      HeartbeatKey key = partitionConsumptionState.getOrCreateCachedHeartbeatKey(region);
      IngestionTimestampEntry entry = getLeaderHeartbeatTimeStamps().get(key);
      if (entry == null) {
        continue;
      }
      found = true;
      // Skip separate RT topic as it is not tracked towards replication latency goal.
      if (Utils.isSeparateTopicRegion(region)) {
        continue;
      }
      if (!entry.consumedFromUpstream) {
        if (shouldLogLag) {
          LOGGER.info(
              "Replica: {} has not received any valid leader heartbeat from region: {}.",
              partitionConsumptionState.getReplicaId(),
              region);
        }
        maxLag = Long.MAX_VALUE;
      } else {
        long heartbeatLag = currentTimestamp - entry.heartbeatTimestamp;
        if (shouldLogLag) {
          LOGGER.info(
              "Replica: {} has leader heartbeat lag: {}ms from region: {}.",
              partitionConsumptionState.getReplicaId(),
              heartbeatLag,
              region);
        }
        maxLag = Math.max(maxLag, heartbeatLag);
      }
    }
    if (!found) {
      if (shouldLogLag) {
        LOGGER.warn("Replica: {} leader lag entry not found.", partitionConsumptionState.getReplicaId());
      }
      return Long.MAX_VALUE;
    }
    return maxLag;
  }

  long getReplicaFollowerHeartbeatLag(
      PartitionConsumptionState partitionConsumptionState,
      boolean shouldLogLag,
      long currentTimestamp) {
    HeartbeatKey key = partitionConsumptionState.getOrCreateCachedHeartbeatKey(getLocalRegionName());
    IngestionTimestampEntry followerReplicaTimestamp = getFollowerHeartbeatTimeStamps().get(key);
    if (followerReplicaTimestamp == null) {
      if (shouldLogLag) {
        LOGGER.warn("Replica: {} follower lag entry not found.", partitionConsumptionState.getReplicaId());
      }
      return Long.MAX_VALUE;
    }
    if (!followerReplicaTimestamp.consumedFromUpstream) {
      if (shouldLogLag) {
        LOGGER.info(
            "Replica: {} has not received any valid follower heartbeat from local region.",
            partitionConsumptionState.getReplicaId());
      }
      return Long.MAX_VALUE;
    }
    long heartbeatLag = currentTimestamp - followerReplicaTimestamp.heartbeatTimestamp;
    if (shouldLogLag) {
      LOGGER.info(
          "Replica: {} has follower heartbeat lag: {}ms from local region.",
          partitionConsumptionState.getReplicaId(),
          heartbeatLag);
    }
    return heartbeatLag;
  }

  ReadOnlyStoreRepository getMetadataRepository() {
    return metadataRepository;
  }

  Duration getMaxWaitForVersionInfo() {
    return maxWaitForVersionInfo;
  }

  /**
   * Record a leader record-level timestamp using a pre-built HeartbeatKey.
   * Avoids HeartbeatKey allocation and hash computation on the per-record hot path.
   */
  public void recordLeaderRecordTimestamp(HeartbeatKey key, Long timestamp, boolean isReadyToServe) {
    if (!recordLevelTimestampEnabled) {
      return;
    }

    recordIngestionTimestamp(key, timestamp, leaderHeartbeatTimeStamps, isReadyToServe, false, false);

    if (perRecordOtelMetricsEnabled) {
      long delay = System.currentTimeMillis() - timestamp;
      versionStatsReporter.emitPerRecordLeaderOtelMetric(key.storeName, key.version, key.region, delay);
    }
  }

  /**
   * Record a follower record-level timestamp using a pre-built HeartbeatKey.
   * Avoids HeartbeatKey allocation and hash computation on the per-record hot path.
   */
  public void recordFollowerRecordTimestamp(HeartbeatKey key, Long timestamp, boolean isReadyToServe) {
    if (!recordLevelTimestampEnabled) {
      return;
    }

    recordIngestionTimestamp(key, timestamp, followerHeartbeatTimeStamps, isReadyToServe, true, false);

    if (perRecordOtelMetricsEnabled) {
      long delay = System.currentTimeMillis() - timestamp;
      versionStatsReporter
          .emitPerRecordFollowerOtelMetric(key.storeName, key.version, key.region, delay, isReadyToServe);
    }
  }

  /**
   * Record a leader heartbeat timestamp using a pre-built HeartbeatKey.
   * Avoids HeartbeatKey allocation and hash computation on the per-record hot path.
   */
  public void recordLeaderHeartbeat(HeartbeatKey key, Long timestamp, boolean isReadyToServe) {
    recordIngestionTimestamp(key, timestamp, leaderHeartbeatTimeStamps, isReadyToServe, false, true);
  }

  /**
   * Record a follower heartbeat timestamp using a pre-built HeartbeatKey.
   * Avoids HeartbeatKey allocation and hash computation on the per-record hot path.
   */
  public void recordFollowerHeartbeat(HeartbeatKey key, Long timestamp, boolean isReadyToServe) {
    recordIngestionTimestamp(key, timestamp, followerHeartbeatTimeStamps, isReadyToServe, true, true);
  }

  /**
   * Internal recording method to record ingestion timestamps from both heartbeat control messages and regular data
   * records.
   *
   * @param isHeartbeatMessage true if this is a heartbeat control message, false if it's a regular data record
   *
   * When isHeartbeatMessage=true (heartbeat control messages):
   *   - heartbeatTimestamp is always updated to the new timestamp
   *   - recordTimestamp = max(timestamp, existing recordTimestamp)
   *
   * When isHeartbeatMessage=false (regular data records):
   *   - heartbeatTimestamp is preserved (not updated)
   *   - recordTimestamp = max(timestamp, existing recordTimestamp)
   */
  private void recordIngestionTimestamp(
      HeartbeatKey key,
      Long timestamp,
      Map<HeartbeatKey, IngestionTimestampEntry> heartbeatTimestamps,
      boolean isReadyToServe,
      boolean retainHighestTimeStamp,
      boolean isHeartbeatMessage) {
    heartbeatTimestamps.computeIfPresent(key, (k, currentEntry) -> {
      // If we are retaining only the highest timestamp for a given heartbeat, if the current held heartbeat
      // is of a higher value AND was an entry was consumed (not a placeholder value by the process) then
      // we will No-Op in favor of retaining that higher timestamp. This behavior is specific to follower
      // nodes because the intent of this metric is to only show the lag of the follower relative to the leader
      if (retainHighestTimeStamp && isHeartbeatMessage && currentEntry.heartbeatTimestamp > timestamp
          && currentEntry.consumedFromUpstream) {
        // No-Op for heartbeat messages when current is higher
      } else if (isHeartbeatMessage) {
        // Heartbeat message: update in-place, recordTimestamp = max of all timestamps
        currentEntry.heartbeatTimestamp = timestamp;
        currentEntry.recordTimestamp = Math.max(timestamp, currentEntry.recordTimestamp);
        currentEntry.readyToServe = isReadyToServe;
        currentEntry.consumedFromUpstream = true;
      } else {
        // Regular data record: preserve heartbeat timestamp, update recordTimestamp in-place
        currentEntry.recordTimestamp = Math.max(timestamp, currentEntry.recordTimestamp);
        currentEntry.readyToServe = isReadyToServe;
        currentEntry.consumedFromUpstream = true;
      }
      return currentEntry;
    });
  }

  protected Map<HeartbeatKey, IngestionTimestampEntry> getLeaderHeartbeatTimeStamps() {
    return leaderHeartbeatTimeStamps;
  }

  protected Map<HeartbeatKey, IngestionTimestampEntry> getFollowerHeartbeatTimeStamps() {
    return followerHeartbeatTimeStamps;
  }

  protected void recordLags(
      Map<HeartbeatKey, IngestionTimestampEntry> heartbeatTimestamps,
      ReportLagFunction lagFunction) {
    for (Map.Entry<HeartbeatKey, IngestionTimestampEntry> entry: heartbeatTimestamps.entrySet()) {
      HeartbeatKey key = entry.getKey();
      lagFunction.apply(
          key.storeName,
          key.version,
          key.region,
          entry.getValue().heartbeatTimestamp,
          entry.getValue().readyToServe);
    }
  }

  protected void recordRecordLags(
      Map<HeartbeatKey, IngestionTimestampEntry> heartbeatTimestamps,
      ReportLagFunction lagFunction) {
    for (Map.Entry<HeartbeatKey, IngestionTimestampEntry> entry: heartbeatTimestamps.entrySet()) {
      HeartbeatKey key = entry.getKey();
      lagFunction.apply(
          key.storeName,
          key.version,
          key.region,
          entry.getValue().recordTimestamp,
          entry.getValue().readyToServe);
    }
  }

  protected void record() {
    // Record heartbeat message delays
    recordLags(
        leaderHeartbeatTimeStamps,
        ((storeName, version, region, heartbeatTs, isReadyToServe) -> versionStatsReporter
            .recordLeaderLag(storeName, version, region, heartbeatTs)));
    recordLags(
        followerHeartbeatTimeStamps,
        ((storeName, version, region, heartbeatTs, isReadyToServe) -> versionStatsReporter
            .recordFollowerLag(storeName, version, region, heartbeatTs, isReadyToServe)));

    // Record record-level delays via OTel (if record-level timestamp tracking is enabled)
    if (recordLevelTimestampEnabled) {
      recordRecordLags(
          leaderHeartbeatTimeStamps,
          ((storeName, version, region, recordTs, isReadyToServe) -> versionStatsReporter
              .recordLeaderRecordLag(storeName, version, region, recordTs)));
      recordRecordLags(
          followerHeartbeatTimeStamps,
          ((storeName, version, region, recordTs, isReadyToServe) -> versionStatsReporter
              .recordFollowerRecordLag(storeName, version, region, recordTs, isReadyToServe)));
    }
  }

  void checkAndMaybeLogHeartbeatDelayMap(Map<HeartbeatKey, IngestionTimestampEntry> heartbeatTimestamps) {
    if (getKafkaStoreIngestionService() == null) {
      // Service not initialized yet, skip logging
      return;
    }
    long currentTimestamp = System.currentTimeMillis();
    boolean isLeader = heartbeatTimestamps == getLeaderHeartbeatTimeStamps();
    for (Map.Entry<HeartbeatKey, IngestionTimestampEntry> entry: heartbeatTimestamps.entrySet()) {
      HeartbeatKey key = entry.getKey();
      long heartbeatTs = entry.getValue().heartbeatTimestamp;
      long lag = currentTimestamp - heartbeatTs;
      if (lag > DEFAULT_STALE_HEARTBEAT_LOG_THRESHOLD_MILLIS && entry.getValue().readyToServe) {
        String replicaId = key.getReplicaId();
        String leaderOrFollower = isLeader ? "leader" : "follower";
        LOGGER.warn(
            "Replica: {}, region: {} is having {} heartbeat lag: {}, latest heartbeat: {}, current timestamp: {}",
            replicaId,
            key.region,
            leaderOrFollower,
            lag,
            heartbeatTs,
            currentTimestamp);
        getKafkaStoreIngestionService()
            .attemptToPrintIngestionInfoFor(key.storeName, key.version, key.partition, key.region);
        /**
         * Here we don't consider whether it is current version or not, as it will need extra logic to extract.
         * In this layer, we will filter out untracked region (separated realtime region).
         * We will delegate to KafkaStoreIngestionService to determine whether it takes this request as it has
         * information about SIT current version.
         */
        if (getServerConfig().isLagBasedReplicaAutoResubscribeEnabled()
            && TimeUnit.SECONDS.toMillis(getServerConfig().getLagBasedReplicaAutoResubscribeThresholdInSeconds()) < lag
            && !Utils.isSeparateTopicRegion(key.region)) {
          getKafkaStoreIngestionService().maybeAddResubscribeRequest(key.storeName, key.version, key.partition);
        }
      }
    }
  }

  protected void checkAndMaybeLogHeartbeatDelay() {
    checkAndMaybeLogHeartbeatDelayMap(leaderHeartbeatTimeStamps);
    checkAndMaybeLogHeartbeatDelayMap(followerHeartbeatTimeStamps);
  }

  /**
   * The cleanup targets resources that have been deleted or are no longer assigned; i.e. the corresponding lag monitor
   * will be removed from both the leader and follower maps. To avoid a scenario where a resource is unassigned and then
   * quickly reassigned to the same node; potentially causing a race condition in which the cleanup thread mistakenly
   * removes the lag monitor, we wait for DEFAULT_LAG_MONITOR_CLEANUP_CYCLE cycles before actually removing the entry.
   * This should prevent other race conditions caused by out of sync customized view too.
   */
  void checkAndMaybeCleanupLagMonitor() {
    if (customizedViewRepository == null && customizedViewRepositoryFuture != null) {
      if (customizedViewRepositoryFuture.isDone()) {
        try {
          customizedViewRepository = customizedViewRepositoryFuture.get();
          LOGGER.info("Customized view repository is ready, starting lag monitor cleanup");
        } catch (Exception e) {
          LOGGER.error("Failed to get customized view repository for lag monitor cleanup", e);
        }
      } else {
        LOGGER.info("Customized view repository not ready yet, skipping lag monitor cleanup");
        return;
      }
    }
    // We assume the heartbeat maps are maintained correctly and no partition should exist in both leader and follower
    // map. If it does it will accelerate the cleanup process if the node can no longer be found in the assignment
    cleanupStoreVersionPartitionMap(leaderHeartbeatTimeStamps);
    cleanupStoreVersionPartitionMap(followerHeartbeatTimeStamps);
  }

  private void cleanupStoreVersionPartitionMap(Map<HeartbeatKey, IngestionTimestampEntry> heartbeatTimestamps) {
    if (customizedViewRepository == null) {
      // Defensive coding, we should already have verified that the customized view repository is ready
      return;
    }
    // Group entries by (store, version, partition) since cleanup checks partition assignments per version
    Map<String, Map<Integer, Set<Integer>>> storeVersionPartitions = new HashMap<>();
    for (HeartbeatKey key: heartbeatTimestamps.keySet()) {
      storeVersionPartitions.computeIfAbsent(key.storeName, k -> new HashMap<>())
          .computeIfAbsent(key.version, k -> new HashSet<>())
          .add(key.partition);
    }
    for (Map.Entry<String, Map<Integer, Set<Integer>>> storeEntry: storeVersionPartitions.entrySet()) {
      String storeName = storeEntry.getKey();
      for (Map.Entry<Integer, Set<Integer>> versionEntry: storeEntry.getValue().entrySet()) {
        int versionNum = versionEntry.getKey();
        PartitionAssignment partitionAssignment = null;
        boolean isResourceDeleted = false;
        try {
          partitionAssignment =
              customizedViewRepository.getPartitionAssignments(Version.composeKafkaTopic(storeName, versionNum));
        } catch (VeniceNoHelixResourceException noHelixResourceException) {
          isResourceDeleted = true;
        }
        for (int partition: versionEntry.getValue()) {
          boolean lingerReplica = isResourceDeleted;
          String replicaId = Utils.getReplicaId(Version.composeKafkaTopic(storeName, versionNum), partition);
          if (!isResourceDeleted) {
            Set<String> instanceIdSet = partitionAssignment.getPartition(partition)
                .getAllInstancesSet()
                .stream()
                .map(Instance::getNodeId)
                .collect(Collectors.toSet());
            if (instanceIdSet.contains(nodeId)) {
              // Replica is still assigned to this node based on locally cached customized view
              cleanupHeartbeatMap.remove(replicaId);
            } else {
              lingerReplica = true;
            }
          }
          if (lingerReplica) {
            cleanupHeartbeatMap.compute(replicaId, (k, v) -> {
              // Replica is not assigned to this node based on locally cached customized view
              // See comments in {@link #addLeaderLagMonitor(Version, int)} for race condition explanations
              if (v == null) {
                return 1;
              } else if (v + 1 >= lagMonitorCleanupCycle) {
                removeLagMonitor(new VersionImpl(storeName, versionNum, ""), partition);
                LOGGER.warn(
                    "Removing lingering replica: {} from heartbeat monitoring service because it is no longer assigned to this node: {}",
                    replicaId,
                    nodeId);
                return null;
              } else {
                return v + 1;
              }
            });
          }
        }
      }
    }
  }

  // For unit testing
  Map<String, Integer> getCleanupHeartbeatMap() {
    return this.cleanupHeartbeatMap;
  }

  AggregatedHeartbeatLagEntry getMaxHeartbeatLag(
      long currentTimestamp,
      Map<HeartbeatKey, IngestionTimestampEntry> heartbeatTimestamps) {
    long minHeartbeatTimestampForCurrentVersion = Long.MAX_VALUE;
    long minHeartbeatTimestampForNonCurrentVersion = Long.MAX_VALUE;
    // Cache current version per store to avoid repeated lookups
    Map<String, Integer> storeCurrentVersions = new HashMap<>();
    for (Map.Entry<HeartbeatKey, IngestionTimestampEntry> entry: heartbeatTimestamps.entrySet()) {
      HeartbeatKey key = entry.getKey();
      int currentVersion = storeCurrentVersions.computeIfAbsent(key.storeName, storeName -> {
        Store store = getMetadataRepository().getStore(storeName);
        if (store == null) {
          LOGGER.debug("Store: {} not found in repository", storeName);
          return -1;
        }
        return store.getCurrentVersion();
      });
      if (currentVersion == -1) {
        continue;
      }
      long heartbeatTs = entry.getValue().heartbeatTimestamp;
      // The current version heartbeat lag will only be tracking serving current version, not bootstrapping ones.
      if (currentVersion == key.version && entry.getValue().readyToServe) {
        minHeartbeatTimestampForCurrentVersion = Math.min(minHeartbeatTimestampForCurrentVersion, heartbeatTs);
      } else {
        minHeartbeatTimestampForNonCurrentVersion = Math.min(minHeartbeatTimestampForNonCurrentVersion, heartbeatTs);
      }
    }
    return new AggregatedHeartbeatLagEntry(
        currentTimestamp - minHeartbeatTimestampForCurrentVersion,
        currentTimestamp - minHeartbeatTimestampForNonCurrentVersion);
  }

  public AggregatedHeartbeatLagEntry getMaxLeaderHeartbeatLag() {
    return getMaxHeartbeatLag(System.currentTimeMillis(), leaderHeartbeatTimeStamps);
  }

  public AggregatedHeartbeatLagEntry getMaxFollowerHeartbeatLag() {
    return getMaxHeartbeatLag(System.currentTimeMillis(), followerHeartbeatTimeStamps);
  }

  @FunctionalInterface
  interface ReportLagFunction {
    void apply(String storeName, int version, String region, long lag, boolean isReadyToServe);
  }

  private class HeartbeatReporterThread extends Thread {
    private final LogContext logContext;

    HeartbeatReporterThread(VeniceServerConfig serverConfig) {
      super("Ingestion-Heartbeat-Reporter-Service-Thread");
      setDaemon(true);
      this.logContext = serverConfig.getLogContext();
    }

    @Override
    public void run() {
      LogContext.setLogContext(logContext);
      boolean exceptionThrown = false;
      while (heartbeatReporterThreadIsRunning.get()) {
        try {
          if (exceptionThrown) {
            TimeUnit.SECONDS.sleep(DEFAULT_REPORTER_THREAD_SLEEP_INTERVAL_SECONDS);
          }
          heartbeatMonitoringServiceStats.recordReporterHeartbeat();
          record();
          TimeUnit.SECONDS.sleep(DEFAULT_REPORTER_THREAD_SLEEP_INTERVAL_SECONDS);
          exceptionThrown = false;
        } catch (InterruptedException e) {
          // We've received an interrupt which is to be expected, so we'll just leave the loop and log
          break;
        } catch (Exception e) {
          exceptionThrown = true;
          LOGGER.error("Received exception from Ingestion-Heartbeat-Reporter-Service-Thread", e);
          heartbeatMonitoringServiceStats.recordHeartbeatExceptionCount();
        } catch (Throwable throwable) {
          exceptionThrown = true;
          LOGGER.error("Received exception from Ingestion-Heartbeat-Reporter-Service-Thread", throwable);
        }
      }
      LOGGER.info("Heartbeat lag metric reporting thread stopped. Shutting down...");
    }
  }

  private class HeartbeatLagCleanupAndLoggingThread extends Thread {
    private final LogContext logContext;

    HeartbeatLagCleanupAndLoggingThread(VeniceServerConfig serverConfig) {
      super("Ingestion-Heartbeat-Lag-Logging-Service-Thread");
      setDaemon(true);
      this.logContext = serverConfig.getLogContext();
    }

    @Override
    public void run() {
      LogContext.setLogContext(logContext);
      boolean exceptionThrown = false;
      while (heartbeatLagCleanupAndLoggingThreadIsRunning.get()) {
        try {
          if (exceptionThrown) {
            TimeUnit.SECONDS.sleep(DEFAULT_LAG_LOGGING_THREAD_SLEEP_INTERVAL_SECONDS);
          }
          heartbeatMonitoringServiceStats.recordLoggerHeartbeat();
          checkAndMaybeCleanupLagMonitor();
          checkAndMaybeLogHeartbeatDelay();
          TimeUnit.SECONDS.sleep(DEFAULT_LAG_LOGGING_THREAD_SLEEP_INTERVAL_SECONDS);
          exceptionThrown = false;
        } catch (InterruptedException e) {
          // We've received an interrupt which is to be expected, so we'll just leave the loop and log
          break;
        } catch (Exception e) {
          exceptionThrown = true;
          LOGGER.error("Received exception from Ingestion-Heartbeat-Lag-Logging-Service-Thread", e);
          heartbeatMonitoringServiceStats.recordHeartbeatExceptionCount();
        } catch (Throwable throwable) {
          exceptionThrown = true;
          LOGGER
              .error("Received non-exception throwable from Ingestion-Heartbeat-Lag-Logging-Service-Thread", throwable);
        }
      }
      LOGGER.info("Heartbeat lag logging thread stopped. Shutting down...");
    }
  }

  String getLocalRegionName() {
    return localRegionName;
  }

  Set<String> getRegionNames() {
    return regionNames;
  }

  public void setKafkaStoreIngestionService(KafkaStoreIngestionService kafkaStoreIngestionService) {
    this.kafkaStoreIngestionService = kafkaStoreIngestionService;
  }

  KafkaStoreIngestionService getKafkaStoreIngestionService() {
    return kafkaStoreIngestionService;
  }

  VeniceServerConfig getServerConfig() {
    return serverConfig;
  }
}
