package com.linkedin.venice.router.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;

import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Count;

public class SecurityStats extends AbstractVeniceStats {
  private final Sensor sslErrorCount;
  private final Sensor sslSuccessCount;

  public SecurityStats(MetricsRepository repository, String name) {
    super(repository, name);
    this.sslErrorCount = registerSensor("ssl_error", new Count());
    this.sslSuccessCount = registerSensor("ssl_success", new Count());
  }

  public void recordSslError() {
    this.sslErrorCount.record();
  }

  public void recordSslSuccess() {
    this.sslSuccessCount.record();
  }
}
