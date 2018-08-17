package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.OccurrenceRate;


/**
 * for measuring requests and quota rejections for each store
 */
public class ServerQuotaUsageStats extends AbstractVeniceStats {

  private final Sensor requested;
  private final Sensor rejected;

  public ServerQuotaUsageStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    requested = registerSensor("QuotaRcuRequested", new OccurrenceRate());
    rejected = registerSensor("QuotaRcuRejected", new OccurrenceRate());
  }

  /**
   * @param rcu The number of Read Capacity Units that the allowed request cost
   */
  public void recordAllowed(long rcu){
    requested.record(rcu);
  }

  /**
   *
   * @param rcu The number of Read Capacity Units tha the rejected request would have cost
   */
  public void recordRejected(long rcu){
    requested.record(rcu);
    rejected.record(rcu);
  }
}
