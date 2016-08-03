package com.linkedin.venice.router.stats;

import io.tehuti.metrics.MetricsRepository;
import org.apache.log4j.Logger;

public class RouterStoreStats extends AbstractRouterStats {

  private static Logger logger = Logger.getLogger(RouterStoreStats.class);

  public RouterStoreStats(MetricsRepository metricsRepository, String prefix) {
    super(metricsRepository, prefix);
  }
}
