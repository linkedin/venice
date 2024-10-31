package com.linkedin.venice.stats;

import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Rate;


/**
 * for measuring requests and quota rejections for each store
 */
public class ServerQuotaUsageStats extends AbstractVeniceStats {
  private final Sensor requestedQPS; // requested query per second
  private final Sensor requestedKPS; // requested key per second
  private final Sensor rejectedQPS; // rejected query per second
  private final Sensor rejectedKPS; // rejected key per second
  private final Sensor allowedUnintentionallyKPS; // allowed KPS unintentionally due to error or insufficient info
  private final Sensor usageRatioSensor; // requested qps divided by nodes quota responsibility

  private long nodeQpsResponsibility = 0;

  public ServerQuotaUsageStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    requestedQPS = registerSensor("quota_request", new Rate());
    requestedKPS = registerSensor("quota_request_key_count", new Rate());
    rejectedQPS = registerSensor("quota_rejected_request", new Rate());
    rejectedKPS = registerSensor("quota_rejected_key_count", new Rate());
    allowedUnintentionallyKPS = registerSensor("quota_unintentionally_allowed_key_count", new Count());
    usageRatioSensor =
        registerSensor(new AsyncGauge((ignored, ignored2) -> getReadQuotaUsageRatio(), "quota_requested_usage_ratio"));
  }

  /**
   * @param rcu The number of Read Capacity Units that the allowed request cost
   */
  public void recordAllowed(long rcu) {
    requestedQPS.record();
    requestedKPS.record(rcu);
  }

  /**
   *
   * @param rcu The number of Read Capacity Units tha the rejected request would have cost
   */
  public void recordRejected(long rcu) {
    requestedQPS.record();
    requestedKPS.record(rcu);
    rejectedQPS.record();
    rejectedKPS.record(rcu);
  }

  public void recordAllowedUnintentionally(long rcu) {
    allowedUnintentionallyKPS.record(rcu);
  }

  public void setNodeQuotaResponsibility(long nodeQpsResponsibility) {
    this.nodeQpsResponsibility = nodeQpsResponsibility;
  }

  /**
   * @return the ratio of the read quota usage to the node's quota responsibility
   */
  private Double getReadQuotaUsageRatio() {
    if (nodeQpsResponsibility < 1) {
      return Double.NaN;
    }
    MetricsRepository metricsRepository = getMetricsRepository();
    Metric metric = metricsRepository.getMetric(requestedKPS.name() + ".Rate");
    if (metric == null) {
      return Double.NaN;
    }

    return metric.value() / nodeQpsResponsibility;
  }
}
