package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.OccurrenceRate;


public class ServerConnectionStats extends AbstractVeniceStats {
  public static final String ROUTER_CONNECTION_COUNT_GAUGE = "router_connection_count";
  public static final String CLIENT_CONNECTION_COUNT_GAUGE = "client_connection_count";

  private final Sensor routerConnectionCountSensor;
  private final Sensor clientConnectionCountSensor;

  private long routerConnectionCount;
  private long clientConnectionCount;

  public ServerConnectionStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    routerConnectionCountSensor = registerSensorIfAbsent(
        ROUTER_CONNECTION_COUNT_GAUGE,
        new Gauge(() -> routerConnectionCount),
        new OccurrenceRate());
    clientConnectionCountSensor = registerSensorIfAbsent(
        CLIENT_CONNECTION_COUNT_GAUGE,
        new Gauge(() -> clientConnectionCount),
        new OccurrenceRate());
  }

  public void incrementRouterConnectionCount() {
    routerConnectionCount++;
    routerConnectionCountSensor.record(routerConnectionCount);
  }

  public void decrementRouterConnectionCount() {
    routerConnectionCount--;
    routerConnectionCountSensor.record(routerConnectionCount);
  }

  public void incrementClientConnectionCount() {
    clientConnectionCount++;
    clientConnectionCountSensor.record(clientConnectionCount);
  }

  public void decrementClientConnectionCount() {
    clientConnectionCount--;
    clientConnectionCountSensor.record(clientConnectionCount);
  }
}
