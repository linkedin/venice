package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;


public abstract class AbstractVeniceStatsReporter<STATS> extends AbstractVeniceStats {
  private STATS stats;
  /**
   * Stats that are recorded into based on the version's role (current/future) at record time, rather than being
   * re-pointed on version swap like {@link #stats}. Null when role-scoped stats are not enabled for this reporter.
   */
  private volatile STATS roleStats;
  protected String storeName;

  public AbstractVeniceStatsReporter(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, storeName);
    this.storeName = storeName;
    registerStats();
  }

  protected abstract void registerStats();

  protected void registerConditionalStats() {
    // default implementation is no-op
  }

  protected void unregisterStats() {
    super.unregisterAllSensors();
  }

  public void setStats(STATS stats) {
    this.stats = stats;
  }

  public STATS getStats() {
    return stats;
  }

  public void setRoleStats(STATS roleStats) {
    this.roleStats = roleStats;
  }

  public STATS getRoleStats() {
    return roleStats;
  }
}
