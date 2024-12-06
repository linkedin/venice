package com.linkedin.davinci.stats.ingestion.heartbeat;

import com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType;
import com.linkedin.davinci.kafka.consumer.ReplicaHeartbeatInfo;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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

  private static final Logger LOGGER = LogManager.getLogger(HeartbeatMonitoringService.class);

  private final Thread reportingThread;
  private final Thread lagLoggingThread;

  private final Set<String> regionNames;
  private final String localRegionName;

  // store -> version -> partition -> region -> (timestamp, RTS)
  private final Map<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> followerHeartbeatTimeStamps;
  private final Map<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> leaderHeartbeatTimeStamps;
  HeartbeatVersionedStats versionStatsReporter;

  public HeartbeatMonitoringService(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      Set<String> regionNames,
      String localRegionName) {
    this.regionNames = regionNames.stream().filter(x -> !Utils.isSeparateTopicRegion(x)).collect(Collectors.toSet());
    this.localRegionName = localRegionName;
    this.reportingThread = new HeartbeatReporterThread();
    this.lagLoggingThread = new HeartbeatLagLoggingThread();
    this.followerHeartbeatTimeStamps = new VeniceConcurrentHashMap<>();
    this.leaderHeartbeatTimeStamps = new VeniceConcurrentHashMap<>();
    this.versionStatsReporter = new HeartbeatVersionedStats(
        metricsRepository,
        metadataRepository,
        () -> new HeartbeatStat(new MetricConfig(), regionNames),
        (aMetricsRepository, storeName, clusterName) -> new HeartbeatStatReporter(
            aMetricsRepository,
            storeName,
            regionNames),
        leaderHeartbeatTimeStamps,
        followerHeartbeatTimeStamps);
  }

  private synchronized void initializeEntry(
      Map<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> heartbeatTimestamps,
      Version version,
      int partition,
      boolean isFollower) {
    // We don't monitor heartbeat lag for non-hybrid versions
    if (version.getHybridStoreConfig() == null) {
      return;
    }
    heartbeatTimestamps.computeIfAbsent(version.getStoreName(), storeKey -> new VeniceConcurrentHashMap<>())
        .computeIfAbsent(version.getNumber(), versionKey -> new VeniceConcurrentHashMap<>())
        .computeIfAbsent(partition, partitionKey -> {
          Map<String, HeartbeatTimeStampEntry> regionTimestamps = new VeniceConcurrentHashMap<>();
          if (version.isActiveActiveReplicationEnabled() && !isFollower) {
            for (String region: regionNames) {
              regionTimestamps.put(region, new HeartbeatTimeStampEntry(System.currentTimeMillis(), false, false));
            }
          } else {
            regionTimestamps
                .put(localRegionName, new HeartbeatTimeStampEntry(System.currentTimeMillis(), false, false));
          }
          return regionTimestamps;
        });
  }

  private synchronized void removeEntry(
      Map<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> heartbeatTimestamps,
      Version version,
      int partition) {
    heartbeatTimestamps.computeIfPresent(version.getStoreName(), (storeKey, versionMap) -> {
      versionMap.computeIfPresent(version.getNumber(), (versionKey, partitionMap) -> {
        partitionMap.remove(partition);
        return partitionMap;
      });
      return versionMap;
    });
  }

  /**
   * Adds monitoring for a follower partition of a given version. This request is ignored if the version
   * isn't hybrid.
   *
   * @param version the version to monitor lag for
   * @param partition the partition to monitor lag for
   */
  public void addFollowerLagMonitor(Version version, int partition) {
    initializeEntry(followerHeartbeatTimeStamps, version, partition, true);
    removeEntry(leaderHeartbeatTimeStamps, version, partition);
  }

  /**
   * Adds monitoring for a leader partition of a given version. This request is ignored if the version
   * isn't hybrid.
   *
   * @param version the version to monitor lag for
   * @param partition the partition to monitor lag for
   */
  public void addLeaderLagMonitor(Version version, int partition) {
    initializeEntry(leaderHeartbeatTimeStamps, version, partition, false);
    removeEntry(followerHeartbeatTimeStamps, version, partition);
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
      Map<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> heartbeatTimestampMap,
      String leaderState,
      long currentTimestamp,
      String versionTopicName,
      int partitionFilter,
      boolean filterLagReplica) {
    Map<String, ReplicaHeartbeatInfo> result = new VeniceConcurrentHashMap<>();
    for (Map.Entry<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> storeName: heartbeatTimestampMap
        .entrySet()) {
      for (Map.Entry<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>> version: storeName.getValue()
          .entrySet()) {
        for (Map.Entry<Integer, Map<String, HeartbeatTimeStampEntry>> partition: version.getValue().entrySet()) {
          for (Map.Entry<String, HeartbeatTimeStampEntry> region: partition.getValue().entrySet()) {
            String topicName = Version.composeKafkaTopic(storeName.getKey(), version.getKey());
            long heartbeatTs = region.getValue().timestamp;
            long lag = currentTimestamp - heartbeatTs;
            if (!versionTopicName.equals(topicName)) {
              continue;
            }
            if (partitionFilter >= 0 && partitionFilter != partition.getKey()) {
              continue;
            }
            if (filterLagReplica && lag < DEFAULT_STALE_HEARTBEAT_LOG_THRESHOLD_MILLIS) {
              continue;
            }
            String replicaId =
                Utils.getReplicaId(Version.composeKafkaTopic(storeName.getKey(), version.getKey()), partition.getKey());
            ReplicaHeartbeatInfo replicaHeartbeatInfo = new ReplicaHeartbeatInfo(
                replicaId,
                region.getKey(),
                leaderState,
                region.getValue().readyToServe,
                heartbeatTs,
                lag);
            result.put(replicaId + "-" + region.getKey(), replicaHeartbeatInfo);
          }
        }
      }
    }
    return result;
  }

  @Override
  public boolean startInner() throws Exception {
    reportingThread.start();
    lagLoggingThread.start();
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    reportingThread.interrupt();
    lagLoggingThread.interrupt();
  }

  /**
   * Record a leader heartbeat timestamp for a given partition of a store version from a specific region.
   *
   * @param store the store this heartbeat is for
   * @param version the version this heartbeat is for
   * @param partition the partition this heartbeat is for
   * @param region the region this heartbeat is from
   * @param timestamp the time of this heartbeat
   * @param isReadyToServe has this partition been marked ready to serve?  This determines how the metric is reported
   */
  public void recordLeaderHeartbeat(
      String store,
      int version,
      int partition,
      String region,
      Long timestamp,
      boolean isReadyToServe) {
    recordHeartbeat(store, version, partition, region, timestamp, leaderHeartbeatTimeStamps, isReadyToServe, false);
  }

  /**
   * Record a follower heartbeat timestamp for a given partition of a store version from a specific region.
   *
   * @param store the store this heartbeat is for
   * @param version the version this heartbeat is for
   * @param partition the partition this heartbeat is for
   * @param region the region this heartbeat is from
   * @param timestamp the time of this heartbeat
   * @param isReadyToServe has this partition been marked ready to serve?  This determines how the metric is reported
   */
  public void recordFollowerHeartbeat(
      String store,
      int version,
      int partition,
      String region,
      Long timestamp,
      boolean isReadyToServe) {
    recordHeartbeat(store, version, partition, region, timestamp, followerHeartbeatTimeStamps, isReadyToServe, true);
  }

  private void recordHeartbeat(
      String store,
      int version,
      int partition,
      String region,
      Long timestamp,
      Map<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> heartbeatTimestamps,
      boolean isReadyToServe,
      boolean retainHighestTimeStamp) {
    if (region != null) {
      heartbeatTimestamps.computeIfPresent(store, (storeKey, perVersionMap) -> {
        perVersionMap.computeIfPresent(version, (versionKey, perPartitionMap) -> {
          perPartitionMap.computeIfPresent(partition, (partitionKey, perRegionMap) -> {
            // If we are retaining only the highest timestamp for a given heartbeat, if the current held heartbeat
            // is of a higher value AND was an entry was consumed (not a place holder value by the process) then
            // we will No-Op in favor of retaining that higher timestamp. This behavior is specific to follower
            // nodes because the intent of this metric is to only show the lag of the follower relative to the leader
            if (retainHighestTimeStamp && perRegionMap.get(region) != null
                && perRegionMap.get(region).timestamp > timestamp && perRegionMap.get(region).consumedFromUpstream) {
              // No-Op
            } else {
              // record the heartbeat time stamp
              perRegionMap.put(region, new HeartbeatTimeStampEntry(timestamp, isReadyToServe, true));
            }
            return perRegionMap;
          });
          return perPartitionMap;
        });
        return perVersionMap;
      });
    }
  }

  protected Map<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> getLeaderHeartbeatTimeStamps() {
    return leaderHeartbeatTimeStamps;
  }

  protected Map<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> getFollowerHeartbeatTimeStamps() {
    return followerHeartbeatTimeStamps;
  }

  protected void recordLags(
      Map<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> heartbeatTimestamps,
      ReportLagFunction lagFunction) {
    for (Map.Entry<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> storeName: heartbeatTimestamps
        .entrySet()) {
      for (Map.Entry<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>> version: storeName.getValue()
          .entrySet()) {
        for (Map.Entry<Integer, Map<String, HeartbeatTimeStampEntry>> partition: version.getValue().entrySet()) {
          for (Map.Entry<String, HeartbeatTimeStampEntry> region: partition.getValue().entrySet()) {
            lagFunction.apply(
                storeName.getKey(),
                version.getKey(),
                region.getKey(),
                region.getValue().timestamp,
                region.getValue().readyToServe);
          }
        }
      }
    }
  }

  protected void record() {
    recordLags(
        leaderHeartbeatTimeStamps,
        ((storeName, version, region, heartbeatTs, isReadyToServe) -> versionStatsReporter
            .recordLeaderLag(storeName, version, region, heartbeatTs)));
    recordLags(
        followerHeartbeatTimeStamps,
        ((storeName, version, region, heartbeatTs, isReadyToServe) -> versionStatsReporter
            .recordFollowerLag(storeName, version, region, heartbeatTs, isReadyToServe)));
  }

  protected void checkAndMaybeLogHeartbeatDelayMap(
      Map<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> heartbeatTimestamps) {
    long currentTimestamp = System.currentTimeMillis();
    for (Map.Entry<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> storeName: heartbeatTimestamps
        .entrySet()) {
      for (Map.Entry<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>> version: storeName.getValue()
          .entrySet()) {
        for (Map.Entry<Integer, Map<String, HeartbeatTimeStampEntry>> partition: version.getValue().entrySet()) {
          for (Map.Entry<String, HeartbeatTimeStampEntry> region: partition.getValue().entrySet()) {
            long heartbeatTs = region.getValue().timestamp;
            long lag = currentTimestamp - heartbeatTs;
            if (lag > DEFAULT_STALE_HEARTBEAT_LOG_THRESHOLD_MILLIS && region.getValue().readyToServe) {
              String replicaId = Utils
                  .getReplicaId(Version.composeKafkaTopic(storeName.getKey(), version.getKey()), partition.getKey());
              LOGGER.warn(
                  "Replica: {}, region: {} is having heartbeat lag: {}, latest heartbeat: {}, current timestamp: {}",
                  replicaId,
                  region.getKey(),
                  lag,
                  heartbeatTs,
                  currentTimestamp);
            }
          }
        }
      }
    }
  }

  protected void checkAndMaybeLogHeartbeatDelay() {
    checkAndMaybeLogHeartbeatDelayMap(leaderHeartbeatTimeStamps);
    checkAndMaybeLogHeartbeatDelayMap(followerHeartbeatTimeStamps);
  }

  @FunctionalInterface
  interface ReportLagFunction {
    void apply(String storeName, int version, String region, long lag, boolean isReadyToServe);
  }

  private class HeartbeatReporterThread extends Thread {
    HeartbeatReporterThread() {
      super("Ingestion-Heartbeat-Reporter-Service-Thread");
    }

    @Override
    public void run() {
      while (!Thread.interrupted()) {
        record();
        try {
          TimeUnit.SECONDS.sleep(DEFAULT_REPORTER_THREAD_SLEEP_INTERVAL_SECONDS);
        } catch (InterruptedException e) {
          // We've received an interrupt which is to be expected, so we'll just leave the loop and log
          break;
        }
      }
      LOGGER.info("Heartbeat lag metric reporting thread interrupted!  Shutting down...");
    }
  }

  private class HeartbeatLagLoggingThread extends Thread {
    HeartbeatLagLoggingThread() {
      super("Ingestion-Heartbeat-Lag-Logging-Service-Thread");
    }

    @Override
    public void run() {
      while (!Thread.interrupted()) {
        checkAndMaybeLogHeartbeatDelay();
        try {
          TimeUnit.SECONDS.sleep(DEFAULT_LAG_LOGGING_THREAD_SLEEP_INTERVAL_SECONDS);
        } catch (InterruptedException e) {
          // We've received an interrupt which is to be expected, so we'll just leave the loop and log
          break;
        }
      }
      LOGGER.info("Heartbeat lag logging thread interrupted!  Shutting down...");
    }
  }
}
