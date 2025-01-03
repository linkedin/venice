package com.linkedin.venice.stats;

import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Rate;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * For measuring quota requests and rejections for a given store.
 * Specifically not using {@link com.linkedin.davinci.stats.VeniceVersionedStats} due to sophisticated needs:
 *   1. Only requestedQPS, requestedKPS, and (usageRatio) nodeKpsResponsibility need to be versioned. We'd still like to
 *      report rejections at the store level, regardless if it came from current or backup version due to stale routing.
 *   2. VeniceVersionedStats is tracking current and future. However, for the purpose of quota we only care about
 *      current and backup.
 */
public class ServerReadQuotaUsageStats extends AbstractVeniceStats {
  private final Sensor currentRequestedQPS; // requested query per second for current version
  private final Sensor backupRequestedQPS; // requested query per second for backup version
  private final Sensor currentRequestedKPS; // requested key per second for current version
  private final Sensor backupRequestedKPS; // requested key per second for backup version
  private final Sensor rejectedQPS; // rejected query per second
  private final Sensor rejectedKPS; // rejected key per second
  private final Sensor allowedUnintentionallyKPS; // allowed KPS unintentionally due to error or insufficient info
  private final Sensor usageRatioSensor; // requested kps divided by nodes quota responsibility
  private final VeniceConcurrentHashMap<Integer, ServerReadQuotaVersionedStats> versionedStats =
      new VeniceConcurrentHashMap<>();
  private final AtomicInteger currentVersion = new AtomicInteger(0);
  private final AtomicInteger backupVersion = new AtomicInteger(0);
  private final Time time;
  private final MetricConfig metricConfig;

  public ServerReadQuotaUsageStats(MetricsRepository metricsRepository, String name) {
    this(metricsRepository, name, new SystemTime());
  }

  public ServerReadQuotaUsageStats(MetricsRepository metricsRepository, String name, Time time) {
    super(metricsRepository, name);
    currentRequestedQPS = registerSensor(
        new AsyncGauge((ignored, ignored2) -> getVersionedRequestedQPS(currentVersion.get()), "current_quota_request"));
    backupRequestedQPS = registerSensor(
        new AsyncGauge((ignored, ignored2) -> getVersionedRequestedQPS(backupVersion.get()), "backup_quota_request"));
    currentRequestedKPS = registerSensor(
        new AsyncGauge(
            (ignored, ignored2) -> getVersionedRequestedKPS(currentVersion.get()),
            "current_quota_request_key_count"));
    backupRequestedKPS = registerSensor(
        new AsyncGauge(
            (ignored, ignored2) -> getVersionedRequestedKPS(backupVersion.get()),
            "backup_quota_request_key_count"));
    rejectedQPS = registerSensor("quota_rejected_request", new Rate());
    rejectedKPS = registerSensor("quota_rejected_key_count", new Rate());
    allowedUnintentionallyKPS = registerSensor("quota_unintentionally_allowed_key_count", new Count());
    usageRatioSensor =
        registerSensor(new AsyncGauge((ignored, ignored2) -> getReadQuotaUsageRatio(), "quota_requested_usage_ratio"));
    this.time = time;
    metricConfig = new MetricConfig(); // use default configs
  }

  public void setCurrentVersion(int version) {
    int oldCurrentVersion = currentVersion.get();
    if (version != oldCurrentVersion) {
      // Defensive coding since set current version can be called multiple times with the same current version
      currentVersion.compareAndSet(oldCurrentVersion, version);
    }
  }

  public void setBackupVersion(int version) {
    int oldBackupVersion = backupVersion.get();
    if (version != oldBackupVersion) {
      backupVersion.compareAndSet(oldBackupVersion, version);
    }
  }

  public int getCurrentVersion() {
    return currentVersion.get();
  }

  public int getBackupVersion() {
    return backupVersion.get();
  }

  public void removeVersion(int version) {
    versionedStats.remove(version);
  }

  /**
   * @param rcu The number of Read Capacity Units that the allowed request cost
   */
  public void recordAllowed(int version, long rcu) {
    ServerReadQuotaVersionedStats stats = getVersionedStats(version);
    stats.recordReadQuotaRequested(rcu);
  }

  /**
   *
   * @param rcu The number of Read Capacity Units tha the rejected request would have cost
   */
  public void recordRejected(int version, long rcu) {
    ServerReadQuotaVersionedStats stats = getVersionedStats(version);
    stats.recordReadQuotaRequested(rcu);
    rejectedQPS.record();
    rejectedKPS.record(rcu);
  }

  public void recordAllowedUnintentionally(long rcu) {
    allowedUnintentionallyKPS.record(rcu);
  }

  public void setNodeQuotaResponsibility(int version, long nodeKpsResponsibility) {
    ServerReadQuotaVersionedStats stats = getVersionedStats(version);
    stats.setNodeKpsResponsibility(nodeKpsResponsibility);
  }

  private ServerReadQuotaVersionedStats getVersionedStats(int version) {
    return versionedStats.computeIfAbsent(version, (ignored) -> new ServerReadQuotaVersionedStats(time, metricConfig));
  }

  // Package private for testing purpose
  final Double getVersionedRequestedQPS(int version) {
    if (version < 1) {
      return Double.NaN;
    }
    return getVersionedStats(version).getRequestedQPS();
  }

  // Package private for testing purpose
  final Double getVersionedRequestedKPS(int version) {
    if (version < 1) {
      return Double.NaN;
    }
    return getVersionedStats(version).getRequestedKPS();
  }

  /**
   * @return the ratio of the read quota usage to the node's quota responsibility
   */
  final Double getReadQuotaUsageRatio() {
    int version = currentVersion.get();
    ServerReadQuotaVersionedStats stats = versionedStats.get(version);
    if (version < 1 || stats == null) {
      return Double.NaN;
    }
    long nodeKpsResponsibility = stats.getNodeKpsResponsibility();
    if (nodeKpsResponsibility < 1) {
      return Double.NaN;
    }
    return stats.getRequestedKPS() / nodeKpsResponsibility;
  }

  private static class ServerReadQuotaVersionedStats {
    private final Rate requestedKPS;
    private final Rate requestedQPS;
    private final AtomicLong nodeKpsResponsibility = new AtomicLong(0);
    private final Time time;
    private final MetricConfig metricConfig;

    public ServerReadQuotaVersionedStats(Time time, MetricConfig metricConfig) {
      this.time = time;
      this.metricConfig = metricConfig;
      requestedKPS = new Rate();
      requestedKPS.init(metricConfig, time.getMilliseconds());
      requestedQPS = new Rate();
      requestedQPS.init(metricConfig, time.getMilliseconds());
    }

    public void recordReadQuotaRequested(long rcu) {
      requestedKPS.record(rcu, time.getMilliseconds());
      requestedQPS.record(1, time.getMilliseconds());
    }

    public void setNodeKpsResponsibility(long nodeKpsResponsibility) {
      this.nodeKpsResponsibility.set(nodeKpsResponsibility);
    }

    public double getRequestedKPS() {
      return requestedKPS.measure(metricConfig, time.getMilliseconds());
    }

    public double getRequestedQPS() {
      return requestedQPS.measure(metricConfig, time.getMilliseconds());
    }

    public long getNodeKpsResponsibility() {
      return nodeKpsResponsibility.get();
    }
  }
}
