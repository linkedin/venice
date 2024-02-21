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

  // This default value SHOULD be a number that will trip an alert. Alerting on this should be duration based,
  // so starting off with a large value is fine that is cleared once we get the next heartbeat
  public long DEFAULT_SENTINEL_HEARTBEAT_TIMESTAMP = System.currentTimeMillis() - 21600000; // 6 hours ago

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
        (aMetricsRepository, s) -> new HeartbeatStatReporter(aMetricsRepository, s, regionNames));
  }

  private synchronized void initializeEntry(
      Map<String, Map<Integer, Map<Integer, Map<String, Long>>>> heartbeatTimestamps,
      Version version,
      int partition) {
    // We don't monitor heartbeat lag for non hybrid versions
    if (version.getHybridStoreConfig() == null) {
      return;
    }
    if (heartbeatTimestamps.get(version.getStoreName()) == null) {
      heartbeatTimestamps.put(version.getStoreName(), new VeniceConcurrentHashMap<>());
    }
    if (heartbeatTimestamps.get(version.getStoreName()).get(version.getNumber()) == null) {
      heartbeatTimestamps.get(version.getStoreName()).put(version.getNumber(), new VeniceConcurrentHashMap<>());
    }
    if (heartbeatTimestamps.get(version.getStoreName()).get(version.getNumber()).get(partition) == null) {
      Map<String, Long> regionTimestamps = new VeniceConcurrentHashMap<>();
      if (version.isActiveActiveReplicationEnabled()) {
        for (String region: regionNames) {
          regionTimestamps.put(region, DEFAULT_SENTINEL_HEARTBEAT_TIMESTAMP);
        }
      } else {
        regionTimestamps.put(localRegionName, DEFAULT_SENTINEL_HEARTBEAT_TIMESTAMP);
      }
      heartbeatTimestamps.get(version.getStoreName()).get(version.getNumber()).put(partition, regionTimestamps);
    }
  }

  private synchronized void removeEntry(
      Map<String, Map<Integer, Map<Integer, Map<String, Long>>>> heartbeatTimestamps,
      Version version,
      int partition) {
    if (heartbeatTimestamps.get(version.getStoreName()) != null) {
      if (heartbeatTimestamps.get(version.getStoreName()).get(version.getNumber()) != null) {
        if (heartbeatTimestamps.get(version.getStoreName()).get(version.getNumber()).get(partition) != null) {
          heartbeatTimestamps.get(version.getStoreName()).get(version.getNumber()).remove(partition);
        }
      }
    }
  }

  /**
   * Adds monitoring for a follower partition of a given version. This request is ignored if the version
   * isn't hybrid.
   *
   * @param version the version to monitor lag for
   * @param partition the partition to monitor lag for
   */
  public void addFollowerLagMonitor(Version version, int partition) {
    initializeEntry(followerHeartbeatTimeStamps, version, partition);
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
    initializeEntry(leaderHeartbeatTimeStamps, version, partition);
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
    if (heartbeatTimestamps.get(store) != null) {
      if (heartbeatTimestamps.get(store).get(version) != null) {
        if (heartbeatTimestamps.get(store).get(version).get(partition) != null) {
          if (region != null) {
            heartbeatTimestamps.get(store).get(version).get(partition).put(region, timestamp);
          }
        }
      }
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
        ((storeName, version, region, lag) -> versionStatsReporter.recordLeaderLag(storeName, version, region, lag)));
    recordLags(
        followerHeartbeatTimeStamps,
        ((storeName, version, region, lag) -> versionStatsReporter.recordFollowerLag(storeName, version, region, lag)));
  }

  @FunctionalInterface
  interface ReportLagFunction {
    void apply(String storeName, int version, String region, Long lag);
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
