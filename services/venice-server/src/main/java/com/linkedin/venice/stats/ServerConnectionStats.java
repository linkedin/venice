package com.linkedin.venice.stats;

import static com.linkedin.davinci.stats.ServerConnectionOtelMetricEntity.CONNECTION_ACTIVE_COUNT;
import static com.linkedin.davinci.stats.ServerConnectionOtelMetricEntity.CONNECTION_REQUEST_COUNT;
import static com.linkedin.davinci.stats.ServerConnectionOtelMetricEntity.CONNECTION_SETUP_TIME;

import com.linkedin.venice.stats.dimensions.VeniceConnectionSource;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;


public class ServerConnectionStats extends AbstractVeniceStats {
  public static final String ROUTER_CONNECTION_REQUEST = "router_connection_request";
  public static final String ROUTER_CONNECTION_COUNT_GAUGE = "router_connection_count";
  public static final String CLIENT_CONNECTION_REQUEST = "client_connection_request";
  public static final String CLIENT_CONNECTION_COUNT_GAUGE = "client_connection_count";
  public static final String CONNECTION_REQUEST = "connection_request";
  public static final String NEW_CONNECTION_SETUP_LATENCY = "new_connection_setup_latency";

  private final Sensor connectionRequestSensor;

  private final AtomicLong routerConnectionCount = new AtomicLong();
  private final AtomicLong clientConnectionCount = new AtomicLong();

  // OTel: UP_DOWN_COUNTER, no Tehuti binding (Tehuti uses AsyncGauge on AtomicLong)
  private final MetricEntityStateOneEnum<VeniceConnectionSource> activeCountOtel;
  // OTel: shared instrument, each bound to its respective Tehuti OccurrenceRate sensor
  private final MetricEntityStateOneEnum<VeniceConnectionSource> routerRequestCountOtel;
  private final MetricEntityStateOneEnum<VeniceConnectionSource> clientRequestCountOtel;
  // OTel: joint Tehuti+OTel histogram
  private final MetricEntityStateBase setupTimeOtel;

  enum TehutiMetricName implements TehutiMetricNameEnum {
    ROUTER_CONNECTION_REQUEST, CLIENT_CONNECTION_REQUEST, NEW_CONNECTION_SETUP_LATENCY
  }

  public ServerConnectionStats(MetricsRepository metricsRepository, String name, String clusterName) {
    super(metricsRepository, name);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(clusterName).build();
    VeniceOpenTelemetryMetricsRepository otelRepository = otelData.getOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();

    // Tehuti AsyncGauges for active connection counts
    registerSensorIfAbsent(
        new AsyncGauge((ignored, ignored2) -> routerConnectionCount.get(), ROUTER_CONNECTION_COUNT_GAUGE));
    registerSensorIfAbsent(
        new AsyncGauge((ignored, ignored2) -> clientConnectionCount.get(), CLIENT_CONNECTION_COUNT_GAUGE));

    activeCountOtel = MetricEntityStateOneEnum.create(
        CONNECTION_ACTIVE_COUNT.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        VeniceConnectionSource.class);

    routerRequestCountOtel = MetricEntityStateOneEnum.create(
        CONNECTION_REQUEST_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        TehutiMetricName.ROUTER_CONNECTION_REQUEST,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        VeniceConnectionSource.class);

    clientRequestCountOtel = MetricEntityStateOneEnum.create(
        CONNECTION_REQUEST_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        TehutiMetricName.CLIENT_CONNECTION_REQUEST,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        VeniceConnectionSource.class);

    // Tehuti only — OTel total derived at query time
    connectionRequestSensor = registerSensorIfAbsent(CONNECTION_REQUEST, new OccurrenceRate());

    setupTimeOtel = MetricEntityStateBase.create(
        CONNECTION_SETUP_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        TehutiMetricName.NEW_CONNECTION_SETUP_LATENCY,
        Arrays.asList(TehutiUtils.getPercentileStatWithAvgAndMax(getName(), NEW_CONNECTION_SETUP_LATENCY)),
        baseDimensionsMap,
        otelData.getBaseAttributes());
  }

  public void incrementRouterConnectionCount() {
    routerConnectionCount.incrementAndGet();
    routerRequestCountOtel.record(1, VeniceConnectionSource.ROUTER);
    activeCountOtel.record(1, VeniceConnectionSource.ROUTER);
  }

  public void decrementRouterConnectionCount() {
    routerConnectionCount.decrementAndGet();
    activeCountOtel.record(-1, VeniceConnectionSource.ROUTER);
  }

  public void incrementClientConnectionCount() {
    clientConnectionCount.incrementAndGet();
    clientRequestCountOtel.record(1, VeniceConnectionSource.CLIENT);
    activeCountOtel.record(1, VeniceConnectionSource.CLIENT);
  }

  public void decrementClientConnectionCount() {
    clientConnectionCount.decrementAndGet();
    activeCountOtel.record(-1, VeniceConnectionSource.CLIENT);
  }

  public void newConnectionRequest() {
    connectionRequestSensor.record();
  }

  /**
   * Record the latency from the start of channel initialization to SSL handshake completion.
   */
  public void recordNewConnectionSetupLatency(double latencyMs) {
    setupTimeOtel.record(latencyMs);
  }
}
