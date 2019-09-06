package com.linkedin.venice.router.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;

import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import java.util.function.Supplier;


public class SecurityStats extends AbstractVeniceStats {
  private final Supplier<Integer> secureConnectionCountSupplier;
  private final Sensor sslErrorCount;
  private final Sensor sslSuccessCount;
  private final Sensor sslLiveConnectionCount;

  public SecurityStats(MetricsRepository repository, String name, Supplier<Integer> secureConnectionCountSupplier) {
    super(repository, name);
    this.secureConnectionCountSupplier = secureConnectionCountSupplier;
    this.sslErrorCount = registerSensor("ssl_error", new Count());
    this.sslSuccessCount = registerSensor("ssl_success", new Count());
    this.sslLiveConnectionCount = registerSensor("ssl_connection_count", new Avg(), new Max(), new Min());
  }

  public void recordSslError() {
    this.sslErrorCount.record();
    recordLiveConnectionCount();
  }

  public void recordSslSuccess() {
    this.sslSuccessCount.record();
    recordLiveConnectionCount();
  }

  /**
   * This function will be triggered in every ssl event.
   */
  private void recordLiveConnectionCount() {
    this.sslLiveConnectionCount.record(secureConnectionCountSupplier.get());
  }
}
