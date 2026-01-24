package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.ThreadPoolStats;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * Stats for cross-TP parallel processing thread pool.
 */
public class CrossTpProcessingStats extends ThreadPoolStats {
  public CrossTpProcessingStats(MetricsRepository metricsRepository, ThreadPoolExecutor threadPool) {
    super(metricsRepository, threadPool, "CrossTpProcessing");
  }
}
