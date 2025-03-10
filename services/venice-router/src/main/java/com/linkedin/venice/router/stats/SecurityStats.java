package com.linkedin.venice.router.stats;

import com.linkedin.alpini.netty4.ssl.SslInitializer;
import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;


public class SecurityStats extends AbstractVeniceStats {
  private final Sensor sslErrorCount;
  private final Sensor sslSuccessCount;
  private final Sensor liveConnectionCount;
  private final Sensor nonSslConnectionCount;

  public SecurityStats(MetricsRepository repository, String name) {
    super(repository, name);
    this.sslErrorCount = registerSensor("ssl_error", new Count());
    this.sslSuccessCount = registerSensor("ssl_success", new Count());
    this.liveConnectionCount = registerSensor("connection_count", new Avg(), new Max(), new Min());
    this.nonSslConnectionCount = registerSensor("non_ssl_request_count", new Avg(), new Max());
  }

  public void recordSslError() {
    this.sslErrorCount.record();
  }

  public void recordSslSuccess() {
    this.sslSuccessCount.record();
  }

  /**
   * Record all HTTP requests received in routers.
   */
  public void recordNonSslRequest() {
    this.nonSslConnectionCount.record();
  }

  /**
   * This function will be triggered in {@link com.linkedin.alpini.netty4.handlers.ConnectionControlHandler}.
   */
  public void recordLiveConnectionCount(int connectionCount) {
    this.liveConnectionCount.record(connectionCount);
  }

  public void registerSslHandshakeSensors(SslInitializer sslInitializer) {
    registerSensor(
        new AsyncGauge(
            (ignored1, ignored2) -> sslInitializer.getHandshakesStarted()
                - (sslInitializer.getHandshakesSuccessful() + sslInitializer.getHandshakesFailed()),
            "pending_ssl_handshake_count"));
    registerSensor(
        new AsyncGauge(
            (ignored1, ignored2) -> sslInitializer.getHandshakesFailed(),
            "total_failed_ssl_handshake_count"));
  }
}
