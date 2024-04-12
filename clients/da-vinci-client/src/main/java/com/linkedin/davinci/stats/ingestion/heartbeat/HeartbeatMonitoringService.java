package com.linkedin.davinci.stats.ingestion.heartbeat;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
  private final Thread reportingThread;
  private static final Logger LOGGER = LogManager.getLogger(HeartbeatMonitoringService.class);
  public static final int DEFAULT_REPORTER_THREAD_SLEEP_INTERVAL_SECONDS = 60;

  private final Set<String> regionNames;
  private final String localRegionName;

  // store -> version -> partition -> region -> timestamp
  private final Map<String, Map<Integer, Map<Integer, Map<String, Long>>>> followerHeartbeatTimeStamps;
  private final Map<String, Map<Integer, Map<Integer, Map<String, Long>>>> leaderHeartbeatTimeStamps;
  HeartbeatVersionedStats versionStatsReporter;

  public HeartbeatMonitoringService(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      Set<String> regionNames,
      String localRegionName) {
    this.regionNames = regionNames;
    this.localRegionName = localRegionName;
    this.reportingThread = new HeartbeatReporterThread();
    followerHeartbeatTimeStamps = new VeniceConcurrentHashMap<>();
    leaderHeartbeatTimeStamps = new VeniceConcurrentHashMap<>();
    versionStatsReporter = new HeartbeatVersionedStats(
        metricsRepository,
        metadataRepository,
        () -> new HeartbeatStat(new MetricConfig(), regionNames),
        (aMetricsRepository, storeName) -> new HeartbeatStatReporter(aMetricsRepository, storeName, regionNames),
        leaderHeartbeatTimeStamps,
        followerHeartbeatTimeStamps);
  }

  private synchronized void initializeEntry(
      Map<String, Map<Integer, Map<Integer, Map<String, Long>>>> heartbeatTimestamps,
      Version version,
      int partition,
      boolean isFollower) {
    // We don't monitor heartbeat lag for non hybrid versions
    if (version.getHybridStoreConfig() == null) {
      return;
    }
    heartbeatTimestamps.computeIfAbsent(version.getStoreName(), storeKey -> new VeniceConcurrentHashMap<>())
        .computeIfAbsent(version.getNumber(), versionKey -> new VeniceConcurrentHashMap<>())
        .computeIfAbsent(partition, partitionKey -> {
          Map<String, Long> regionTimestamps = new VeniceConcurrentHashMap<>();
          if (version.isActiveActiveReplicationEnabled() && !isFollower) {
            for (String region: regionNames) {
              regionTimestamps.put(region, System.currentTimeMillis());
            }
          } else {
            regionTimestamps.put(localRegionName, System.currentTimeMillis());
          }
          return regionTimestamps;
        });
  }

  private synchronized void removeEntry(
      Map<String, Map<Integer, Map<Integer, Map<String, Long>>>> heartbeatTimestamps,
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

  @Override
  public boolean startInner() throws Exception {
    reportingThread.start();
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    reportingThread.interrupt();
  }

  /**
   * Record a leader heartbeat timestamp for a given partition of a store version from a specific region.
   *
   * @param store the store this heartbeat is for
   * @param version the version this heartbeat is for
   * @param partition the partition this heartbeat is for
   * @param region the region this heartbeat is from
   * @param timestamp the time of this heartbeat
   */
  public void recordLeaderHeartbeat(String store, int version, int partition, String region, Long timestamp) {
    recordHeartbeat(store, version, partition, region, timestamp, leaderHeartbeatTimeStamps);
  }

  /**
   * Record a follower heartbeat timestamp for a given partition of a store version from a specific region.
   *
   * @param store the store this heartbeat is for
   * @param version the version this heartbeat is for
   * @param partition the partition this heartbeat is for
   * @param region the region this heartbeat is from
   * @param timestamp the time of this heartbeat
   */
  public void recordFollowerHeartbeat(String store, int version, int partition, String region, Long timestamp) {
    recordHeartbeat(store, version, partition, region, timestamp, followerHeartbeatTimeStamps);
  }

  private void recordHeartbeat(
      String store,
      int version,
      int partition,
      String region,
      Long timestamp,
      Map<String, Map<Integer, Map<Integer, Map<String, Long>>>> heartbeatTimestamps) {
    if (region != null) {
      heartbeatTimestamps.computeIfPresent(store, (storeKey, perVersionMap) -> {
        perVersionMap.computeIfPresent(version, (versionKey, perPartitionMap) -> {
          perPartitionMap.computeIfPresent(partition, (partitionKey, perRegionMap) -> {
            perRegionMap.put(region, timestamp);
            return perRegionMap;
          });
          return perPartitionMap;
        });
        return perVersionMap;
      });
    }
  }

  protected Map<String, Map<Integer, Map<Integer, Map<String, Long>>>> getLeaderHeartbeatTimeStamps() {
    return leaderHeartbeatTimeStamps;
  }

  protected Map<String, Map<Integer, Map<Integer, Map<String, Long>>>> getFollowerHeartbeatTimeStamps() {
    return followerHeartbeatTimeStamps;
  }

  protected void recordLags(
      Map<String, Map<Integer, Map<Integer, Map<String, Long>>>> heartbeatTimestamps,
      ReportLagFunction lagFunction) {
    for (Map.Entry<String, Map<Integer, Map<Integer, Map<String, Long>>>> storeName: heartbeatTimestamps.entrySet()) {
      for (Map.Entry<Integer, Map<Integer, Map<String, Long>>> version: storeName.getValue().entrySet()) {
        for (Map.Entry<Integer, Map<String, Long>> partition: version.getValue().entrySet()) {
          for (Map.Entry<String, Long> region: partition.getValue().entrySet()) {
            lagFunction.apply(storeName.getKey(), version.getKey(), region.getKey(), region.getValue());
          }
        }
      }
    }
  }

  protected void record() {

    recordLags(
        leaderHeartbeatTimeStamps,
        ((storeName, version, region, heartbeatTs) -> versionStatsReporter
            .recordLeaderLag(storeName, version, region, heartbeatTs)));
    recordLags(
        followerHeartbeatTimeStamps,
        ((storeName, version, region, heartbeatTs) -> versionStatsReporter
            .recordFollowerLag(storeName, version, region, heartbeatTs)));
  }

  @FunctionalInterface
  interface ReportLagFunction {
    void apply(String storeName, int version, String region, long lag);
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
      LOGGER.info("RemoteIngestionRepairService thread interrupted!  Shutting down...");
    }
  }
}
