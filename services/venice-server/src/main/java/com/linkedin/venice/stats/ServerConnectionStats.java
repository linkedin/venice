package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.concurrent.atomic.AtomicLong;


public class ServerConnectionStats extends AbstractVeniceStats {
  public static final String ROUTER_CONNECTION_REQUEST = "router_connection_request";
  public static final String ROUTER_CONNECTION_COUNT_GAUGE = "router_connection_count";
  public static final String CLIENT_CONNECTION_REQUEST = "client_connection_request";
  public static final String CLIENT_CONNECTION_COUNT_GAUGE = "client_connection_count";

  private final Sensor routerConnectionRequestSensor;
  private final Sensor clientConnectionRequestSensor;

  private final AtomicLong routerConnectionCount = new AtomicLong();
  private final AtomicLong clientConnectionCount = new AtomicLong();

  public ServerConnectionStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    registerSensorIfAbsent(
        new AsyncGauge((ignored, ignored2) -> routerConnectionCount.get(), ROUTER_CONNECTION_COUNT_GAUGE));
    routerConnectionRequestSensor = registerSensorIfAbsent(ROUTER_CONNECTION_REQUEST, new OccurrenceRate());
    registerSensorIfAbsent(
        new AsyncGauge((ignored, ignored2) -> clientConnectionCount.get(), CLIENT_CONNECTION_COUNT_GAUGE));
    clientConnectionRequestSensor = registerSensorIfAbsent(CLIENT_CONNECTION_REQUEST, new OccurrenceRate());
  }

  public long incrementRouterConnectionCount() {
    long count = routerConnectionCount.incrementAndGet();
    routerConnectionRequestSensor.record(1);
    return count;
  }

  public long decrementRouterConnectionCount() {
    return routerConnectionCount.decrementAndGet();
  }

  public long incrementClientConnectionCount() {
    long count = clientConnectionCount.incrementAndGet();
    clientConnectionRequestSensor.record(1);
    return count;
  }

  public long decrementClientConnectionCount() {
    return clientConnectionCount.decrementAndGet();
  }
}
