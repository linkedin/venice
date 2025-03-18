package com.linkedin.venice.router.stats;

import com.linkedin.alpini.netty4.ssl.SslInitializer;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.LongAdderRateGauge;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import java.util.concurrent.atomic.AtomicInteger;


public class SecurityStats extends AbstractVeniceStats {
  private final Sensor sslErrorCountSensor;
  private final Sensor sslSuccessCountSensor;
  private final Sensor liveConnectionCountSensor;
  private final Sensor rejectedConnectionCountSensor;
  private final Sensor nonSslConnectionCountSensor;

  private final AtomicInteger activeConnectionCount = new AtomicInteger();

  public SecurityStats(MetricsRepository repository, String name) {
    super(repository, name);
    this.sslErrorCountSensor = registerSensor("ssl_error", new Count());
    this.sslSuccessCountSensor = registerSensor("ssl_success", new Count());
    this.liveConnectionCountSensor = registerSensor("connection_count", new Avg(), new Max());
    registerSensor(new AsyncGauge((ignored1, ignored2) -> activeConnectionCount.get(), "connection_count_gauge"));
    this.nonSslConnectionCountSensor = registerSensor("non_ssl_request_count", new Avg(), new Max());
    rejectedConnectionCountSensor = registerSensor("rejected_connection_count", new LongAdderRateGauge());
  }

  public void recordSslError() {
    this.sslErrorCountSensor.record();
  }

  public void recordSslSuccess() {
    this.sslSuccessCountSensor.record();
  }

  /**
   * Record all HTTP requests received in routers.
   */
  public void recordNonSslRequest() {
    this.nonSslConnectionCountSensor.record();
  }

  /**
   * This function will be triggered in {@link com.linkedin.alpini.netty4.handlers.ConnectionControlHandler}
   * or {@link com.linkedin.alpini.netty4.handlers.ConnectionLimitHandler} depending on the connection handle mode.
   */
  public void recordLiveConnectionCount(int connectionCount) {
    this.activeConnectionCount.set(connectionCount);
    updateConnectionCountInCurrentMetricTimeWindow();
  }

  public void recordRejectedConnectionCount(int rejectedConnectionCount) {
    this.rejectedConnectionCountSensor.record(rejectedConnectionCount);
  }

  /**
   * Calling this function doesn't mean the active connection counter is updated; this function will only update
   * the metric with the connection counter maintained above {@link SecurityStats#activeConnectionCount}
   *
   * Since new connections or inactive connections events might not happen in each one-minute time window, a more
   * active path like data request path will be leveraged to record avg/max connections in each time window.
   */
  public void updateConnectionCountInCurrentMetricTimeWindow() {
    this.liveConnectionCountSensor.record(activeConnectionCount.get());
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
    registerSensor(
        new AsyncGauge(
            (ignored1, ignored2) -> sslInitializer.getHandshakesStarted() - sslInitializer.getHandshakesFailed(),
            "active_ssl_connection_count"));
  }
}
