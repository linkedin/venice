package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Gauge;
import io.tehuti.metrics.stats.Total;


/**
 * for measuring requests and quota rejections for each store
 */
public class ServerQuotaUsageStats extends AbstractVeniceStats {
  private final Sensor requestedQPS; // requested query per second
  private final Sensor requestedKPS; // requested key per second
  private final Sensor rejectedQPS; // rejected query per second
  private final Sensor rejectedKPS; // rejected key per second
  private final Sensor quotaUsageRatio; // quota usage key per second

  public ServerQuotaUsageStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    requestedQPS = registerSensor("quota_rcu_requested", new Count());
    requestedKPS = registerSensor("quota_rcu_requested_key", new Total());
    rejectedQPS = registerSensor("quota_rcu_rejected", new Count());
    rejectedKPS = registerSensor("quota_rcu_rejected_key", new Total());
    quotaUsageRatio = registerSensor("read_quota_usage_ratio", new Gauge());
  }

  /**
   * @param rcu The number of Read Capacity Units that the allowed request cost
   */
  public void recordAllowed(long rcu) {
    requestedQPS.record(rcu);
    requestedKPS.record(rcu);
  }

  /**
   *
   * @param rcu The number of Read Capacity Units tha the rejected request would have cost
   */
  public void recordRejected(long rcu) {
    requestedQPS.record(rcu);
    requestedKPS.record(rcu);
    rejectedQPS.record(rcu);
    rejectedKPS.record(rcu);
  }

  /**
   * @param ratio The number of Read Capacity Units (KPS) used divided by total capacity
   */
  public void recordReadQuotaUsage(double ratio) {
    quotaUsageRatio.record(ratio);
  }
}
