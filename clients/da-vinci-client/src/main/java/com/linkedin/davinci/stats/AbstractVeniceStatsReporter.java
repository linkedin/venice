package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;


public abstract class AbstractVeniceStatsReporter<STATS> extends AbstractVeniceStats {
  private STATS stats;
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
}
