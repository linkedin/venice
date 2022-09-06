package com.linkedin.venice.tehuti;

import io.tehuti.metrics.MetricsRepository;


public interface MetricsAware {
  MetricsRepository getMetricsRepository();
}
