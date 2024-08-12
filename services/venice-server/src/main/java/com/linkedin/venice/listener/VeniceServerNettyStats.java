package com.linkedin.venice.listener;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.concurrent.atomic.AtomicLong;


public class VeniceServerNettyStats extends AbstractVeniceStats {
  private static final String NETTY_SERVER = "netty_server";
  // active concurrent connections
  private static final String ACTIVE_CONNECTIONS = "active_connections";
  private final AtomicLong activeConnections = new AtomicLong();

  public VeniceServerNettyStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    registerSensorIfAbsent(new AsyncGauge((ignored, ignored2) -> activeConnections.get(), ACTIVE_CONNECTIONS));
  }

  public static long getElapsedTimeInMicros(long startTimeNanos) {
    return (System.nanoTime() - startTimeNanos) / 1000;
  }

  public void incrementActiveConnections() {
    activeConnections.incrementAndGet();
  }

  public void decrementActiveConnections() {
    activeConnections.decrementAndGet();
  }
}
