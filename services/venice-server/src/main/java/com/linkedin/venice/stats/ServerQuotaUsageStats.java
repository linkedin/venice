package com.linkedin.venice.stats;

import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Rate;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;


/**
 * for measuring requests and quota rejections for each store
 */
public class ServerQuotaUsageStats extends AbstractVeniceStats {
  private final Sensor requestedQPS; // requested query per second
  private final AtomicReference<Sensor> requestedKPS = new AtomicReference<>(); // requested key per second
  private final Sensor rejectedQPS; // rejected query per second
  private final Sensor rejectedKPS; // rejected key per second
  private final Sensor allowedUnintentionallyKPS; // allowed KPS unintentionally due to error or insufficient info
  private final Sensor usageRatioSensor; // requested kps divided by nodes quota responsibility
  private final Sensor nodeResponsibilitySensor; // kps assigned to the node based on store quota and replica assignment
  private static final String REQUESTED_KPS_SENSOR_NAME = "quota_request_key_count";

  private final AtomicLong nodeKpsResponsibility = new AtomicLong(0);

  public ServerQuotaUsageStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    requestedQPS = registerSensor("quota_request", new Rate());
    requestedKPS.set(registerSensor(REQUESTED_KPS_SENSOR_NAME, new Rate()));
    rejectedQPS = registerSensor("quota_rejected_request", new Rate());
    rejectedKPS = registerSensor("quota_rejected_key_count", new Rate());
    allowedUnintentionallyKPS = registerSensor("quota_unintentionally_allowed_key_count", new Count());
    usageRatioSensor =
        registerSensor(new AsyncGauge((ignored, ignored2) -> getReadQuotaUsageRatio(), "quota_requested_usage_ratio"));
    nodeResponsibilitySensor = registerSensor(
        new AsyncGauge((ignored, ignored2) -> nodeKpsResponsibility.get(), "quota_node_responsibility_kps"));
  }

  /**
   * @param rcu The number of Read Capacity Units that the allowed request cost
   */
  public void recordAllowed(long rcu) {
    requestedQPS.record();
    requestedKPS.get().record(rcu);
  }

  /**
   *
   * @param rcu The number of Read Capacity Units tha the rejected request would have cost
   */
  public void recordRejected(long rcu) {
    requestedQPS.record();
    requestedKPS.get().record(rcu);
    rejectedQPS.record();
    rejectedKPS.record(rcu);
  }

  public void recordAllowedUnintentionally(long rcu) {
    allowedUnintentionallyKPS.record(rcu);
  }

  public void setNodeQuotaResponsibility(long nodeKpsResponsibility) {
    if (this.nodeKpsResponsibility.get() != nodeKpsResponsibility) {
      this.nodeKpsResponsibility.set(nodeKpsResponsibility);
      // We need to reset the KPS rate since we could be using the "old" rate and new node responsibility to calculate
      // the usage ratio and get unexpected spikes. Resetting the KPS rate could still cause usage ratio to dip.
      // However,
      // dips are more accurate/aligned with the internal state since the new token bucket is filled when initialized
      // when we change node responsibility.
      unregisterSensor(requestedKPS.get().name());
      requestedKPS.set(registerSensor(REQUESTED_KPS_SENSOR_NAME, new Rate()));
    }
  }

  /**
   * @return the ratio of the read quota usage to the node's quota responsibility
   */
  private Double getReadQuotaUsageRatio() {
    if (nodeKpsResponsibility.get() < 1) {
      return Double.NaN;
    }
    MetricsRepository metricsRepository = getMetricsRepository();
    Metric metric = metricsRepository.getMetric(requestedKPS.get().name() + ".Rate");
    if (metric == null) {
      return Double.NaN;
    }

    return metric.value() / nodeKpsResponsibility.get();
  }
}
