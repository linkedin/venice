package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;


public class ServerConnectionStats extends AbstractVeniceStats {
  public static final String ROUTER_CONNECTION_COUNT_GAUGE = "router_connection_count_gauge";
  public static final String CLIENT_CONNECTION_COUNT_GAUGE = "client_connection_count_gauge";

  private final Sensor routerConnectionCountSensor;
  private final Sensor clientConnectionCountSensor;

  private int routerConnectionCount;
  private int clientConnectionCount;

  public ServerConnectionStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    routerConnectionCountSensor = registerSensorIfAbsent(ROUTER_CONNECTION_COUNT_GAUGE, new Gauge());
    clientConnectionCountSensor = registerSensorIfAbsent(CLIENT_CONNECTION_COUNT_GAUGE, new Gauge());
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
