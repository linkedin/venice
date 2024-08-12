package com.linkedin.venice.listener;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.concurrent.atomic.AtomicInteger;


public class VeniceServerNettyStats extends AbstractVeniceStats {
  private static final String NETTY_SERVER = "netty_server";
  // active concurrent connections
  private static final String ACTIVE_CONNECTIONS = "active_connections";
  private final AtomicInteger activeConnections = new AtomicInteger();

  private static final String ACTIVE_READ_HANDLER_THREADS = "active_read_handler_threads";
  private final AtomicInteger activeReadHandlerThreads = new AtomicInteger();

  public VeniceServerNettyStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    registerSensorIfAbsent(new AsyncGauge((ignored, ignored2) -> activeConnections.get(), ACTIVE_CONNECTIONS));

    registerSensorIfAbsent(
        new AsyncGauge((ignored, ignored2) -> activeReadHandlerThreads.get(), ACTIVE_READ_HANDLER_THREADS));
  }

  public static long getElapsedTimeInMicros(long startTimeNanos) {
    return (System.nanoTime() - startTimeNanos) / 1000;
  }

  public int incrementActiveReadHandlerThreads() {
    return activeReadHandlerThreads.incrementAndGet();
  }

  public int decrementActiveReadHandlerThreads() {
    return activeReadHandlerThreads.decrementAndGet();
  }

  public int incrementActiveConnections() {
    return activeConnections.incrementAndGet();
  }

  public int decrementActiveConnections() {
    return activeConnections.decrementAndGet();
  }
}
