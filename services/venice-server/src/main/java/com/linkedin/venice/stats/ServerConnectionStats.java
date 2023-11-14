package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.OccurrenceRate;


public class ServerConnectionStats extends AbstractVeniceStats {
  public static final String NEW_ROUTER_CONNECTION_COUNT = "new_router_connection_count";
  public static final String ROUTER_CONNECTION_COUNT_GAUGE = "router_connection_count";
  public static final String NEW_CLIENT_CONNECTION_COUNT = "new_client_connection_count";
  public static final String CLIENT_CONNECTION_COUNT_GAUGE = "client_connection_count";

  private final Sensor routerConnectionCountSensor;
  private final Sensor clientConnectionCountSensor;

  private long routerConnectionCount;
  private long clientConnectionCount;

  public ServerConnectionStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    registerSensorIfAbsent(new AsyncGauge((c, t) -> routerConnectionCount, ROUTER_CONNECTION_COUNT_GAUGE));
    routerConnectionCountSensor = registerSensorIfAbsent(NEW_ROUTER_CONNECTION_COUNT, new OccurrenceRate());
    registerSensorIfAbsent(new AsyncGauge((c, t) -> clientConnectionCount, CLIENT_CONNECTION_COUNT_GAUGE));
    clientConnectionCountSensor = registerSensorIfAbsent(NEW_CLIENT_CONNECTION_COUNT, new OccurrenceRate());
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
