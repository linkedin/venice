package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.HeartbeatMonitoringOtelMetricEntity.HEARTBEAT_MONITORING_EXCEPTION_COUNT;
import static com.linkedin.davinci.stats.HeartbeatMonitoringOtelMetricEntity.HEARTBEAT_MONITORING_HEARTBEAT_COUNT;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.dimensions.VeniceHeartbeatComponent;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.Collections;
import java.util.Map;


public class HeartbeatMonitoringServiceStats extends AbstractVeniceStats {
  private static final String HEARTBEAT_SUFFIX = "-heartbeat-monitor-service";

  /**
   * Tehuti metric names. Hyphenated names are preserved for backward compatibility with existing
   * dashboards — the default {@link TehutiMetricNameEnum#getMetricName()} returns the lowercased
   * enum constant name (which uses underscores), so each constant overrides it to preserve the
   * original hyphenated names.
   */
  enum TehutiMetricName implements TehutiMetricNameEnum {
    HEARTBEAT_MONITOR_SERVICE_EXCEPTION_COUNT("heartbeat-monitor-service-exception-count"),
    HEARTBEAT_REPORTER("heartbeat-reporter"), HEARTBEAT_LOGGER("heartbeat-logger");

    private final String metricName;

    TehutiMetricName(String metricName) {
      this.metricName = metricName;
    }

    @Override
    public String getMetricName() {
      return metricName;
    }
  }

  /**
   * 3 joint Tehuti+OTel metric states mapping to 2 OTel metrics. The reporter and logger heartbeat
   * states share the same OTel instrument ({@code HEARTBEAT_MONITORING_HEARTBEAT_COUNT}) but bind
   * different Tehuti sensors — each {@link MetricEntityStateOneEnum} wraps a distinct Tehuti sensor
   * while contributing to the same OTel counter differentiated by the
   * {@link VeniceHeartbeatComponent} dimension.
   */
  private final MetricEntityStateOneEnum<VeniceHeartbeatComponent> exceptionCountMetrics;
  private final MetricEntityStateOneEnum<VeniceHeartbeatComponent> reporterHeartbeatMetrics;
  private final MetricEntityStateOneEnum<VeniceHeartbeatComponent> loggerHeartbeatMetrics;

  public HeartbeatMonitoringServiceStats(
      MetricsRepository metricsRepository,
      String heartbeatStatPrefix,
      String clusterName) {
    super(metricsRepository, heartbeatStatPrefix + HEARTBEAT_SUFFIX);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(clusterName).build();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();

    this.exceptionCountMetrics = MetricEntityStateOneEnum.create(
        HEARTBEAT_MONITORING_EXCEPTION_COUNT.getMetricEntity(),
        otelData.getOtelRepository(),
        this::registerSensorIfAbsent,
        TehutiMetricName.HEARTBEAT_MONITOR_SERVICE_EXCEPTION_COUNT,
        Collections.singletonList(new Count()),
        baseDimensionsMap,
        VeniceHeartbeatComponent.class);

    this.reporterHeartbeatMetrics = MetricEntityStateOneEnum.create(
        HEARTBEAT_MONITORING_HEARTBEAT_COUNT.getMetricEntity(),
        otelData.getOtelRepository(),
        this::registerSensorIfAbsent,
        TehutiMetricName.HEARTBEAT_REPORTER,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        VeniceHeartbeatComponent.class);

    this.loggerHeartbeatMetrics = MetricEntityStateOneEnum.create(
        HEARTBEAT_MONITORING_HEARTBEAT_COUNT.getMetricEntity(),
        otelData.getOtelRepository(),
        this::registerSensorIfAbsent,
        TehutiMetricName.HEARTBEAT_LOGGER,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        VeniceHeartbeatComponent.class);
  }

  public void recordHeartbeatExceptionCount(VeniceHeartbeatComponent component) {
    exceptionCountMetrics.record(1, component);
  }

  public void recordReporterHeartbeat() {
    reporterHeartbeatMetrics.record(1, VeniceHeartbeatComponent.REPORTER);
  }

  public void recordLoggerHeartbeat() {
    loggerHeartbeatMetrics.record(1, VeniceHeartbeatComponent.LOGGER);
  }
}
