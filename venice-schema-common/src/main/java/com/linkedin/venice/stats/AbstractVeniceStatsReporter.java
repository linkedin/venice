package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;


public abstract class AbstractVeniceStatsReporter<STATS> extends AbstractVeniceStats {
  private STATS stats;

  public AbstractVeniceStatsReporter(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, storeName);
    registerStats();
  }

  protected abstract void registerStats();

  public void setStats(STATS stats) {
    this.stats = stats;
  }

  public STATS getStats() {
    return stats;
  }
}
