package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.concurrent.atomic.AtomicLong;


public class ServerConnectionStats extends AbstractVeniceStats {
  public static final String ROUTER_CONNECTION_REQUEST = "router_connection_request";
  public static final String ROUTER_CONNECTION_COUNT_GAUGE = "router_connection_count";
  public static final String CLIENT_CONNECTION_REQUEST = "client_connection_request";
  public static final String CLIENT_CONNECTION_COUNT_GAUGE = "client_connection_count";
  public static final String CLIENT_CREATE_CHANNEL = "client_create_channel";
  public static final String CLIENT_CLOSE_CHANNEL = "client_close_channel";

  private final Sensor routerConnectionRequestSensor;
  private final Sensor clientConnectionRequestSensor;
  private final Sensor clientDisconnectSensor;
  private final Sensor clientCreateChannelSensor;
  private final Sensor clientCloseChannelSensor;
  private final Sensor activeChannelCountSensor;

  private final AtomicLong routerConnectionCount = new AtomicLong();
  private final AtomicLong clientConnectionCount = new AtomicLong();
  private final AtomicLong clientChannelCount = new AtomicLong();

  public ServerConnectionStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    registerSensorIfAbsent(
        new AsyncGauge((ignored, ignored2) -> routerConnectionCount.get(), ROUTER_CONNECTION_COUNT_GAUGE));
    routerConnectionRequestSensor = registerSensorIfAbsent(ROUTER_CONNECTION_REQUEST, new OccurrenceRate());
    registerSensorIfAbsent(
        new AsyncGauge((ignored, ignored2) -> clientConnectionCount.get(), CLIENT_CONNECTION_COUNT_GAUGE));
    clientConnectionRequestSensor = registerSensorIfAbsent(CLIENT_CONNECTION_REQUEST, new OccurrenceRate());
    clientDisconnectSensor = registerSensorIfAbsent("client_disconnect", new OccurrenceRate());
    clientCreateChannelSensor = registerSensorIfAbsent(CLIENT_CREATE_CHANNEL, new OccurrenceRate());
    clientCloseChannelSensor = registerSensorIfAbsent(CLIENT_CLOSE_CHANNEL, new OccurrenceRate());
    activeChannelCountSensor = registerSensor("active_channel_count", new Max(), new Avg());

  }

  public void incrementRouterConnectionCount() {
    routerConnectionCount.incrementAndGet();
    routerConnectionRequestSensor.record(1);
  }

  public void decrementRouterConnectionCount() {
    routerConnectionCount.decrementAndGet();
  }

  public void incrementClientConnectionCount() {
    clientConnectionCount.incrementAndGet();
    clientConnectionRequestSensor.record(1);
  }

  public void decrementClientConnectionCount() {
    clientConnectionCount.decrementAndGet();
    clientDisconnectSensor.record(1);
  }

  public void incrementClientChannelCount() {
    clientCreateChannelSensor.record(1);
    activeChannelCountSensor.record(clientChannelCount.incrementAndGet());
  }

  public void decrementClientChannelCount() {
    clientCloseChannelSensor.record(1);
    activeChannelCountSensor.record(clientChannelCount.decrementAndGet());
  }
}
