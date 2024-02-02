package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.OccurrenceRate;


public class ServerConnectionStats extends AbstractVeniceStats {
  public static final String ROUTER_CONNECTION_REQUEST = "router_connection_request";
  public static final String ROUTER_CONNECTION_COUNT_GAUGE = "router_connection_count";
  public static final String CLIENT_CONNECTION_REQUEST = "client_connection_request";
  public static final String CLIENT_CONNECTION_COUNT_GAUGE = "client_connection_count";

  private final Sensor routerConnectionRequestSensor;
  private final Sensor clientConnectionRequestSensor;

  private long routerConnectionCount;
  private long clientConnectionCount;

  public ServerConnectionStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    registerSensorIfAbsent(new AsyncGauge((ignored, ignored2) -> routerConnectionCount, ROUTER_CONNECTION_COUNT_GAUGE));
    routerConnectionRequestSensor = registerSensorIfAbsent(ROUTER_CONNECTION_REQUEST, new OccurrenceRate());
    registerSensorIfAbsent(new AsyncGauge((ignored, ignored2) -> clientConnectionCount, CLIENT_CONNECTION_COUNT_GAUGE));
    clientConnectionRequestSensor = registerSensorIfAbsent(CLIENT_CONNECTION_REQUEST, new OccurrenceRate());
  }

  public void incrementRouterConnectionCount() {
    routerConnectionCount++;
    routerConnectionRequestSensor.record(1);
  }

  public void decrementRouterConnectionCount() {
    routerConnectionCount--;
  }

  public void incrementClientConnectionCount() {
    clientConnectionCount++;
    clientConnectionRequestSensor.record(1);
  }

  public void decrementClientConnectionCount() {
    clientConnectionCount--;
  }
}
